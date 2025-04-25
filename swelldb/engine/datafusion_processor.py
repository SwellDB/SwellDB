# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.engine.execution_engine import ExecutionEngine


import pyarrow as pa
import pyarrow.dataset as ds

from pandas import DataFrame
from datafusion import SessionContext, Catalog


class DataFusionEngine(ExecutionEngine):
    def __init__(self):
        self._sc: SessionContext = SessionContext()
        self._materialized_tables: dict[str, LogicalTable] = dict()

    def refresh(self) -> None:
        for table in self._materialized_tables.keys():
            self._sc.deregister_table(table)
        self._materialized_tables = dict()

    def get_tables(self):
        """
        Returns the table names and schemata
        :return:
        """
        tables = self._sc.tables()
        table_schemas = {}
        for table in tables:
            table_schemas[table] = (
                self._sc.table(table).schema().to_string().split("\n")
            )
        return table_schemas

    def register_csv(self, name: str, path: str):
        table_ds: ds.Dataset = ds.dataset(path, format="csv")

        if not self._sc.table_exist(name):
            self._sc.register_dataset(name, table_ds)

    def register_table(self, name: str, df: DataFrame):
        table_ds = ds.dataset(pa.Table.from_pandas(df))

        if not self._sc.table_exist(name):
            self._sc.register_dataset(name, table_ds)

    def deregister_table(self, name: str):
        self._sc.deregister_table(name)

    def sql(self, query: str) -> DataFrame:
        return self._sc.sql(query)
