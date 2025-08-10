# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import Union, List, Dict

import pyarrow as pa

from swelldb.engine.datafusion_processor import DataFusionEngine
from swelldb.table_plan.planner import TableGenPlanner
from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.custom_table import CustomTable
from swelldb.table_plan.table.physical.llm_table import LLMTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.table_plan.table.physical.search_engine_table import SearchEngineTable
from swelldb.table_plan.table.physical.image_table import ImageTable
from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.llm.openai_llm import OpenAILLM
from swelldb.table_plan.meta import SwellDBMeta
from swelldb.table_plan.mode import Mode
from swelldb.util.config import Config

class TableBuilder:
    def __init__(self, swelldb_ctx: "SwellDB"):
        self._meta: SwellDBMeta = SwellDBMeta()
        self._child_table = None
        self.swelldb_ctx = swelldb_ctx
        self.csv_files: List[(str, str)] = []
        self.parquet_files: List[(str, str)] = []

    def set_table_name(self, name: str) -> "TableBuilder":
        self._meta.set_table_name(name)
        return self

    def set_content(self, content: str) -> "TableBuilder":
        self._meta.set_content(content)
        return self

    def set_schema(self, schema: Union[SwellDBSchema, str]) -> "TableBuilder":
        self._meta.set_schema(schema)
        return self

    def set_base_columns(self, base_columns: List[str]) -> "TableBuilder":
        self._meta.set_base_columns(base_columns)
        return self

    def set_table_gen_mode(self, mode: "Mode") -> "TableBuilder":
        self._meta.set_table_gen_mode(mode)
        return self

    def set_operators(self, operators: List[type]) -> "TableBuilder":
        self._meta.set_operators(operators)
        return self

    def set_data(self, data: pa.Table) -> "TableBuilder":
        if self._child_table:
            raise ValueError(
                "Cannot set data when child table is already set. Please use either data or child_table."
            )
        self._meta.set_data(data)
        return self

    def set_child_table(self, table: PhysicalTable) -> "TableBuilder":
        if self._meta.get_data():
            raise ValueError(
                "Cannot set child table when data is already set. Please use either data or child_table."
            )
        self._child_table = table
        return self

    def set_chunk_size(self, chunk_size: int) -> "TableBuilder":
        self._meta.set_chunk_size(chunk_size)
        return self

    def add_csv_file(self, name: str, path: str) -> "TableBuilder":
        self.csv_files.append((name, path))
        return self

    def add_parquet_file(self, name: str, path: str) -> "TableBuilder":
        self.parquet_files.append((name, path))
        return self

    def add_images(self, image_path: str):
        self._meta.add_image(image_path)
        return self

    def build(self):
        """
        Build the table using the provided parameters.
        """
        if self._meta.get_content() is None:
            raise ValueError("Content must be set.")

        if self._meta.get_schema() is None:
            raise ValueError("Schema must be set.")

        for csv_file in self.csv_files:
            name, path = csv_file
            self.swelldb_ctx._execution_engine.register_csv(name=name, path=path)

        tables = self.swelldb_ctx._execution_engine.get_tables()

        return self.swelldb_ctx._create_table(
            meta=self._meta,
            child_table=self._child_table,
            tables=tables,
        )


class SwellDB:
    def __init__(
        self,
        llm: AbstractLLM,
        execution_engine: ExecutionEngine = DataFusionEngine(),
        serper_api_key: str = None,
    ):
        self._execution_engine = execution_engine
        self._llm = llm
        
        # Load config and use environment variables as override
        self._config = Config()
        self._serper_api_key = serper_api_key or self._config.get_serper_api_key()
        
        self._planner = TableGenPlanner(
            llm=llm, execution_engine=execution_engine, serper_api_key=self._serper_api_key
        )

    def table_builder(self) -> TableBuilder:
        """
        Returns a TableBuilder instance to build tables.
        """
        return TableBuilder(self)

    def _create_table(
        self,
        meta: SwellDBMeta,
        child_table: PhysicalTable = None,
        tables: Dict[str, str] = None,
    ) -> PhysicalTable:
        """
        Create a table using the provided metadata.

        Args:
            meta (SwellDBMeta): The metadata for the table.
            child_table (PhysicalTable): Optional child table. Default is None.
            tables (Dict[str, str]): A dictionary of registered tables to be used for the table generation. Default is None.

        Returns:
            PhysicalTable: The created table.

        Examples:
            >>> from swelldb import SwellDB
            >>> from swelldb.llm.openai_llm import OpenAILLM
            >>> from swelldb.table_plan.meta import SwellDBMeta

            >>> swell_ctx = SwellDB(OpenAILLM())
            >>> meta = SwellDBMeta()
            >>> meta.set_table_name("country")
            >>> meta.set_content("a list of all US states")
            >>> meta.set_schema("country_name, president, year")
            >>> tbl = swell_ctx._create_table(meta=meta)
        """
        mode = meta.get_table_gen_mode()
        base_columns = meta.get_base_columns()
        operators = meta.get_operators()
        schema = meta.get_schema()
        data = meta.get_data()

        if mode == Mode.PLANNER and not base_columns:
            raise ValueError(
                "Base columns (base_columns) must be specified in planner mode."
            )

        if mode == Mode.OPERATORS and not operators:
            raise ValueError("A list of operators should provider in operators mode.")

        if mode == Mode.IMAGE and not meta.get_images():
            raise ValueError("Image paths must be specified in image mode. Use add_images() to add image paths.")

        if isinstance(schema, str):
            schema = SwellDBSchema.from_string(schema)

        # Create the logical table
        logical_table = LogicalTable(
            name=meta.get_table_name(),
            prompt=meta.get_content(),
            schema=schema
        )

        # If data is provided, create a child table that includes that data
        if data:
            child_table = CustomTable(
                "data",
                meta=meta,
                child_table=child_table,
            )

        # Experimental
        if mode == Mode.PLANNER:
            table: PhysicalTable = self._planner.create_plan(
                logical_table=logical_table,
                base_columns=base_columns,
                tables=tables
            )
        # Experimental
        elif mode == Mode.OPERATORS:
            table: PhysicalTable = self._planner.create_plan_from_operators(
                logical_table=logical_table,
                meta=meta,
                tables=tables,
            )
        elif mode == Mode.LLM:
            table: PhysicalTable = LLMTable(
                logical_table=logical_table,
                child_table=child_table,
                meta=meta,
                llm=self._llm,
                execution_engine=self._execution_engine,
            )
        elif mode == Mode.SEARCH:
            table: PhysicalTable = SearchEngineTable(
                logical_table=logical_table,
                child_table=child_table,
                meta=meta,
                llm=self._llm,
                execution_engine=self._execution_engine,
            )
        elif mode == Mode.IMAGE:
            table: PhysicalTable = ImageTable(
                logical_table=logical_table,
                child_table=child_table,
                meta=meta,
                llm=self._llm,
                execution_engine=self._execution_engine,
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")

        return table
