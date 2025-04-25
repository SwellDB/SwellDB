# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from overrides import override
from pyarrow import Table

from swelldb.datasource.database_datasource import DatabaseDatasource
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable


class DatabaseTable(PhysicalTable):
    def __init__(
        self,
        logical_table: LogicalTable,
        query: str,
        database_datasource: DatabaseDatasource,
    ):
        super().__init__(logical_table=logical_table, children=[])

        self._query = query
        self._db = database_datasource

    @override
    def materialize(self) -> Table:
        return self._db.sql(self._query)

    @override
    def __str__(self):
        return f'DatabaseTable[schema={self._logical_table.get_schema()}, query="{self._query}"]'
