# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import asyncio
import json
from collections import defaultdict

import math
from typing import List, Dict

import pyarrow as pa
from pyarrow import Table

from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.table.logical.logical_table import LogicalTable

import logging


class PhysicalTable:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        llm: AbstractLLM,
        logical_table: LogicalTable,
        child_table: "PhysicalTable",
        operator_name: str,
        layout: Layout = Layout.ROW(),
        base_columns: List[str] = None,
        chunk_size: int = 10,
    ):
        self._logical_table = logical_table
        self._chunk_size = chunk_size
        self._child_table = child_table
        self._layout: Layout = layout
        self._base_columns: str = base_columns
        self._operator_name: str = operator_name
        self._llm = llm
        self._execution_engine = execution_engine

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        raise NotImplementedError()

    def get_operator_name(self) -> str:
        return self._operator_name

    # TODO: Resolve the issue with the search engine table
    @staticmethod
    def get_columns_prompt(logical_table: LogicalTable, tables: Dict[str, str]) -> str:
        return None

    def get_child_table(self) -> "PhysicalTable":
        return self._child_table

    def _parse_llm_response(self, resp: str, layout: Layout) -> Dict:
        """
        Robustly parse LLM response and extract data.
        
        Args:
            resp: The LLM response string
            layout: The expected layout (COLUMN or ROW)
            
        Returns:
            Dictionary with column data
            
        Raises:
            ValueError: If response cannot be parsed
        """
        try:
            # First, try to parse as JSON
            parsed = json.loads(resp)
            logging.debug(f"Successfully parsed JSON response with keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'Not a dict'}")
            
            if layout == Layout.COLUMN():
                if "columns" in parsed and isinstance(parsed["columns"], dict):
                    # Validate column data structure
                    columns = parsed["columns"]
                    if not columns:
                        logging.warning("Empty columns data in response")
                        return {}
                    
                    # Ensure all values are lists
                    for col_name, values in columns.items():
                        if not isinstance(values, list):
                            logging.warning(f"Column {col_name} is not a list: {type(values)}")
                            columns[col_name] = [str(values)] if values is not None else [""]
                    
                    return columns
                else:
                    logging.warning("Response missing 'columns' key or invalid format")
                    raise ValueError("Invalid column format in response")
                    
            elif layout == Layout.ROW():
                if "rows" in parsed and isinstance(parsed["rows"], list):
                    row_data = parsed["rows"]
                    logging.debug(f"Processing {len(row_data)} rows from response")
                    column_data = defaultdict(list)
                    
                    for row in row_data:
                        if isinstance(row, list):
                            for idx, attr in enumerate(
                                self._logical_table.get_schema().get_attributes()
                            ):
                                if idx < len(row):
                                    # Handle None values and convert to string
                                    value = row[idx]
                                    if value is None:
                                        value = ""
                                    elif not isinstance(value, str):
                                        value = value
                                    column_data[attr.get_name()].append(value)
                                else:
                                    # Fill missing values with empty string
                                    column_data[attr.get_name()].append("")
                        else:
                            logging.warning(f"Invalid row format: {row}, expected list but got {type(row)}")
                            continue
                    
                    return dict(column_data)
                else:
                    logging.warning("Response missing 'rows' key or invalid format")
                    raise ValueError("Invalid row format in response")
            else:
                raise ValueError(f"Unsupported layout: {layout}")
                
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON response: {e}")
            logging.warning(f"Raw response: {resp[:200]}...")
            raise ValueError(f"Response is not valid JSON: {e}")
        except KeyError as e:
            logging.warning(f"Missing required key in response: {e}")
            logging.warning(f"Available keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'Not a dict'}")
            raise ValueError(f"Missing required key: {e}")
        except Exception as e:
            logging.error(f"Unexpected error parsing response: {e}")
            raise ValueError(f"Failed to parse response: {e}")

    def partition_table(self, data: pa.Table) -> List[pa.Table]:
        num_partitions: int = math.ceil(data.num_rows / self._chunk_size)
        partitions: List[pa.Table] = list()

        offset: int = 0
        for i in range(0, num_partitions):
            partition = data.slice(offset, self._chunk_size)
            partitions.append(partition)
            offset += self._chunk_size

        return partitions

    def materialize(self, partitions: int = 1) -> pa.Table:
        final_result: Table = None

        child_result: Table = (
            self._child_table.materialize(partitions) if self._child_table else None
        )

        prompts: List[str] = self.get_prompts(child_result)
        n_prompts: int = len(prompts)

        for idx, prompt in enumerate(prompts):
            logging.info("Processing prompt {}/{}".format(idx + 1, n_prompts))

            resp: str = self._llm.call(prompt)

            logging.info(f"Issuing LLM call with prompt: {prompt}")
            logging.info(f"Response: {resp}")

            try:
                # Use robust parsing method
                column_data = self._parse_llm_response(resp, self._layout)
            except ValueError as e:
                logging.error(f"Failed to parse LLM response: {e}")
                logging.warning(f"Skipping prompt {idx + 1} due to parsing error")
                continue

            # Validate column data before creating table
            if not column_data:
                logging.warning(f"No valid column data extracted from prompt {idx + 1}")
                continue
                
            # Ensure all required columns are present
            required_columns = [attr.get_name() for attr in self._logical_table.get_schema().get_attributes()]
            missing_columns = [col for col in required_columns if col not in column_data]
            
            if missing_columns:
                logging.warning(f"Missing columns in response: {missing_columns}")
                # Fill missing columns with empty values
                max_length = max(len(v) for v in column_data.values()) if column_data else 0
                for col in missing_columns:
                    column_data[col] = [""] * max_length
            
            # Ensure all columns have consistent lengths
            if column_data:
                lengths = [len(v) for v in column_data.values()]
                if len(set(lengths)) > 1:
                    logging.warning(f"Inconsistent column lengths: {lengths}")
                    max_length = max(lengths)
                    # Pad shorter columns with empty strings
                    for col, values in column_data.items():
                        if len(values) < max_length:
                            column_data[col].extend([""] * (max_length - len(values)))

            # Cast the output to the correct schema
            try:
                output_tbl: pa.Table = pa.table(
                    column_data, schema=self._logical_table.get_schema().to_arrow_schema()
                )
            except Exception as e:
                logging.error(f"Failed to create PyArrow table: {e}")
                logging.warning(f"Skipping prompt {idx + 1} due to table creation error")
                continue

            result = output_tbl

            if child_result:
                result = result.join(
                    right_table=child_result,
                    keys=self._base_columns,
                    join_type="inner",
                )

            if not final_result:
                final_result = result
            else:
                final_result = pa.concat_tables(
                    [final_result, result.cast(final_result.schema)]
                )

        # Handle case where no prompts succeeded
        if final_result is None:
            logging.warning("No prompts were successfully processed, returning empty table")
            # Create empty table with correct schema
            empty_data = {attr.get_name(): [] for attr in self._logical_table.get_schema().get_attributes()}
            final_result = pa.table(empty_data, schema=self._logical_table.get_schema().to_arrow_schema())

        return final_result

    def explain(self, space="") -> None:
        print("{}{}".format(space, self.__str__()))
        if self._child_table:
            self._child_table.explain(space + "--")

    def __or__(self, other):
        other._child_table = self
        return other

