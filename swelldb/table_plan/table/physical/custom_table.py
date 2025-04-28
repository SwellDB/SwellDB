# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List

from pyarrow import Table

from swelldb.table_plan.table.physical.physical_table import PhysicalTable


class CustomTable(PhysicalTable):
    def __init__(self, table_name, data, chunk_size, layout, base_columns: List[str]):
        super().__init__(
            table_name,
            [],
            chunk_size=chunk_size,
            layout=layout,
            logical_table=None,
            child_table=None,
            operator_name=None,
            base_columns=base_columns,
        )

        self.data: Table = data

    def materialize(self, partitions: int = 1) -> Table:
        return self.data
