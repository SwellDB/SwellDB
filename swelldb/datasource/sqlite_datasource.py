# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import Dict, List

import pyarrow as pa
import sqlite3
import pandas as pd

from swelldb.datasource.database_datasource import DatabaseDatasource


class SQLiteDatasource(DatabaseDatasource):
    def __init__(self, path: str):
        self.path = path
        self.cursor = sqlite3.connect(path)

    def schema(self) -> Dict[str, List[str]]:
        res = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in res.fetchall()]

        # Fetch column names for each table
        table_columns = {}
        for table in tables:
            res = self.cursor.execute(f"PRAGMA table_info({table})")
            columns = [
                row[1] for row in res.fetchall()
            ]  # Column names are in the second field
            table_columns[table] = columns

        return table_columns

    def sql(self, query: str) -> pa.Table:
        df = pd.read_sql_query(query, self.cursor)
        return pa.table(df)
