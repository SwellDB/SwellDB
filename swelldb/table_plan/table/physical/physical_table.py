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
from pyarrow.lib import Schema, RecordBatch

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

    def transform(self, partition: pa.Table) -> List[RecordBatch]:
        raise NotImplementedError()

    def get_operator_name(self) -> str:
        return self._operator_name

    # TODO: Resolve the issue with the search engine table
    @staticmethod
    def get_columns_prompt(logical_table: LogicalTable, tables: Dict[str, str]) -> str:
        return None

    def get_child_table(self) -> "PhysicalTable":
        return self._child_table

    def get_partitions(self) -> List[pa.Table]:
        data: pa.Table = self.materialize()
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

        if not self.get_child_table():
            children_partitions: List[pa.Table] = [None for _ in range(0, partitions)]
        else:
            children_partitions: List[pa.Table] = (
                self.get_child_table().get_partitions()
            )

        n_partitions: int = len(children_partitions)

        for idx, partition in enumerate(children_partitions):
            logging.info("Processing partition {}/{}".format(idx + 1, n_partitions))

            prompt: str = self.transform(partition)
            resp: str = self._llm.call(prompt)

            logging.info(f"Issuing LLM call with prompt: {prompt}")
            logging.info(f"Response: {resp}")

            # TODO: Create a method for that
            if self._layout == Layout.COLUMN():
                column_data: Dict = json.loads(resp)["columns"]
            elif self._layout == Layout.ROW():
                row_data: Dict = json.loads(resp)["rows"]
                column_data = defaultdict(list)
                for row in row_data:
                    for idx, attr in enumerate(
                        self._logical_table.get_schema().get_attributes()
                    ):
                        column_data[attr.get_name()].append(row[idx])

            # TODO Add sanity checks for the data schema

            # Cast the output to the correct schema
            output_tbl: pa.Table = pa.table(
                column_data, schema=self._logical_table.get_schema().to_arrow_schema()
            )

            if partition:
                result: Table = partition.join(
                    right_table=output_tbl, keys=self._base_columns, join_type="inner"
                )
            else:
                result = output_tbl

            if not final_result:
                final_result = result
            else:
                final_result = pa.concat_tables(
                    [final_result, result.cast(final_result.schema)]
                )

        return final_result

    async def _materialize_parallel(self) -> pa.Table:
        children_partitions: List[pa.Table] = self.get_children()[0].get_partitions()
        n_partitions = len(children_partitions)
        final_partitions: List[pa.Table] = list()

        def transform_partition(idx, partition: pa.Table) -> pa.Table:
            logging.info("Processing partition {}/{}".format(idx + 1, n_partitions))

            try:
                result: pa.Table = self.transform(partition)
                result_schema: Schema = result.schema

                logical_table_schema = set(self._logical_table.get_schema().keys())
                result_schema = set(result_schema.names)

                if logical_table_schema != result_schema:
                    logging.info(
                        "Different schema: {} != {}".format(
                            result_schema, logical_table_schema
                        )
                    )
                    return

                final_partitions.append(result)
                logging.info(
                    "Finished processing partition {}/{}. Input rows: {}, Output rows: {}".format(
                        idx + 1, n_partitions, partition.num_rows, result.num_rows
                    )
                )

            except Exception as e:
                logging.info(
                    "Failed to transform partition {}/{} with error {}".format(
                        idx + 1, n_partitions, e
                    )
                )

        async def working_thread(idx, partition: pa.Table):
            await asyncio.to_thread(transform_partition, idx, partition)

        tasks = [
            working_thread(idx, partition)
            for idx, partition in enumerate(children_partitions)
        ]

        logging.info("Running tasks")

        await asyncio.gather(*tasks)

        final_partitions = [
            f_partition.cast(final_partitions[0].schema)
            for f_partition in final_partitions
        ]

        result: pa.Table = pa.concat_tables(final_partitions)

        logging.info("Done: {}".format(result.num_rows))

        return result

    def materialize_parallel(self) -> pa.Table:
        return asyncio.run(self._materialize_parallel())

    def explain(self, space="") -> None:
        print("{}{}".format(space, self.__str__()))
        if self._child_table:
            self._child_table.explain(space + "--")
