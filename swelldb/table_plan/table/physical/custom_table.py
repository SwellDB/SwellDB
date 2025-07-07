# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List

from pyarrow import Table

from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.table_plan.meta import SwellDBMeta


class CustomTable(PhysicalTable):
    def __init__(
        self,
        table_name: str,
        meta: SwellDBMeta,
        child_table: PhysicalTable = None,
    ):
        super().__init__(
            execution_engine=None,
            logical_table=None,
            child_table=child_table,
            layout=meta.get_layout(),
            operator_name="custom_table",
            llm=None,
            base_columns=meta.get_base_columns(),
            chunk_size=meta.get_chunk_size(),
        )

        self._table_name = table_name
        self._data = meta.get_data()
        self._meta = meta

    def materialize(self, partitions: int = 1) -> Table:
        return self._data

    def __str__(self):
        return (
            f"CustomTable[schema={self._data.column_names}"
        )
