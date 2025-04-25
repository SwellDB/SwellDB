# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import pandas as pd

from swelldb.table_plan.table.logical.logical_table import LogicalTable


class Catalog:
    def __init__(self):
        self._tables: dict[str, LogicalTable] = dict()

    def register_llm_table(self, name: str, prompt: str, schema: dict):
        table: LogicalTable = LogicalTable(name=name, prompt=prompt, schema=schema)

        self._tables[name] = table

    def get_tables(self) -> list[LogicalTable]:
        return self._tables.values()

    def get_tables_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self._tables.items())
