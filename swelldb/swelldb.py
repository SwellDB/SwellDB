# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import Union, List, Dict, Optional, Any
from enum import Enum

import pyarrow as pa

from swelldb.engine.datafusion_processor import DataFusionEngine
from swelldb.table_plan.planner import TableGenPlanner
from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.custom_table import CustomTable
from swelldb.table_plan.table.physical.llm_table import LLMTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.table_plan.table.physical.search_engine_table import SearchEngineTable
from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.llm.openai_llm import OpenAILLM


class TableBuilder:
    def __init__(self, swelldb_ctx: "SwellDB"):
        self._table_name: str = None
        self._content: str = None
        self._schema: Union[SwellDBSchema, str] = None
        self._base_columns: List[str] = None
        self._table_gen_mode: Mode = Mode.LLM
        self._data: pa.Table = None
        self._operators: List[type] = []
        self._chunk_size = 20

        self.swelldb_ctx = swelldb_ctx
        self.csv_files: List[(str, str)] = []
        self.parquet_files: List[(str, str)] = []

    def set_table_name(self, name: str) -> "TableBuilder":
        self._table_name = name
        return self

    def set_content(self, content: str) -> "TableBuilder":
        self._content = content
        return self

    def set_schema(self, schema: Union[SwellDBSchema, str]) -> "TableBuilder":
        self._schema = schema
        return self

    def set_base_columns(self, base_columns: List[str]) -> "TableBuilder":
        self._base_columns = base_columns
        return self

    def set_table_gen_mode(self, mode: "Mode") -> "TableBuilder":
        self._table_gen_mode = mode
        return self

    def set_operators(self, operators: List[type]):
        op_set = set()

        for operator in operators:
            if operator in op_set:
                raise ValueError(
                    f"Duplicate operator found: {operator}. Each operator should be included only once."
                )
            op_set.add(operator)

        self._operators = operators
        return self

    def set_data(self, data: pa.Table) -> "TableBuilder":
        self._data = data
        return self

    def set_chunk_size(self, chunk_size: int) -> "TableBuilder":
        self._chunk_size = chunk_size

        return self

    def add_csv_file(self, name: str, path: str) -> "TableBuilder":
        self.csv_files.append((name, path))
        return self

    def add_parquet_file(self, name: str, path: str) -> "TableBuilder":
        self.parquet_files.append((name, path))
        return self

    def build(self):
        """
        Build the table using the provided parameters.
        """
        if self._content is None:
            raise ValueError("Content must be set.")

        if self._schema is None:
            raise ValueError("Schema must be set.")

        for csv_file in self.csv_files:
            name, path = csv_file
            self.swelldb_ctx._execution_engine.register_csv(name=name, path=path)

        tables = self.swelldb_ctx._execution_engine.get_tables()

        return self.swelldb_ctx._create_table(
            name=self._table_name,
            content=self._content,
            schema=self._schema,
            chunk_size=self._chunk_size,
            operators=self._operators,
            base_columns=self._base_columns,
            mode=self._table_gen_mode,
            tables=tables,
            data=self._data,
        )


class Mode(Enum):
    PLANNER = "planner"
    OPERATORS = "operators"
    LLM = "llm"
    SEARCH = "search"


class SwellDB:
    def __init__(
        self,
        llm: AbstractLLM = OpenAILLM(),
        execution_engine: ExecutionEngine = DataFusionEngine(),
        serper_api_key: str = None,
    ):
        self._execution_engine = execution_engine
        self._llm = llm
        self._serper_api_key = serper_api_key
        self._planner = TableGenPlanner(
            llm=llm, execution_engine=execution_engine, serper_api_key=serper_api_key
        )

    def table_builder(self) -> TableBuilder:
        """
        Returns a TableBuilder instance to build tables.
        """
        return TableBuilder(self)

    def _create_table(
        self,
        name: str,
        content: str,
        schema: Union[SwellDBSchema, str],
        chunk_size: int,
        base_columns: List[str] = None,
        mode: Mode = Mode.LLM,
        operators: List[type] = None,
        layout: Layout = Layout.ROW(),
        data: pa.Table = None,
        tables: Dict[str, str] = None,
    ) -> PhysicalTable:
        """
        Create a table using the given name, content, schema, layout and data.

        Args:
            name (str): The name of the table.
            content (str): The content of the table.
            schema (str): The schema of the table, as a string.
            base_columns (str): The base column to use for the table. Default is None.
            mode (Mode): The mode to use for table creation (LLM or SEARCH).
            layout (Layout): The layout of the table. Default is Layout.ROW().
            data (pa.Table): The data to be used for the table. Default is None.
            tables (Dict[str, str]): A dictionary of registered tables to be used for the table generation. Default is None.

        Returns:
            pa.Table: The created table.

        Examples:
            >>> from swelldb import SwellDB
            >>> from swelldb.llm.openai_llm import OpenAILLM

            >>> swell_ctx = SwellDB(OpenAILLM())
            >>> tbl = swell_ctx.create_table(
            ...     name="country",
            ...     content="a list of all US states",
            ...     schema="country_name, president, year",
            ...     layout=Layout.ROW(),
            ...     data=None
            ... )
        """

        if mode == Mode.PLANNER and not base_columns:
            raise ValueError(
                "Base columns (base_columns) must be specified in planner mode."
            )

        if mode == Mode.OPERATORS and not operators:
            raise ValueError("A list of operators should provider in operators mode.")

        if isinstance(schema, str):
            schema = SwellDBSchema.from_string(schema)

        logical_table = LogicalTable(name=name, prompt=content, schema=schema)

        child_table: PhysicalTable = None

        if data:
            child_table = CustomTable(
                "data",
                data=data,
                chunk_size=chunk_size,
                layout=layout,
                base_columns=base_columns,
            )

        if mode == Mode.PLANNER:
            table: PhysicalTable = self._planner.create_plan(
                logical_table=logical_table, base_columns=base_columns, tables=tables
            )
        elif mode == Mode.OPERATORS:
            table: PhysicalTable = self._planner.create_plan_from_operators(
                logical_table=logical_table,
                base_columns=base_columns,
                tables=tables,
                operators=operators,
            )
        elif mode == Mode.LLM:
            table: PhysicalTable = LLMTable(
                logical_table=logical_table,
                child_table=child_table,
                base_columns=base_columns,
                execution_engine=self._execution_engine,
                llm=self._llm,
                layout=layout,
            )
        elif mode == Mode.SEARCH:
            table: PhysicalTable = SearchEngineTable(
                logical_table=logical_table,
                child_table=child_table,
                base_columns=base_columns,
                execution_engine=self._execution_engine,
                llm=self._llm,
                layout=layout,
                serper_api_key=self._serper_api_key,
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")

        return table
