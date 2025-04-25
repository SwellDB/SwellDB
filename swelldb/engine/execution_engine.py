# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from datafusion import DataFrame


class ExecutionEngine:
    def __init__(self):
        pass

    def sql(self, query: str) -> DataFrame:
        pass

    def get_tables(self):
        pass

    def register_csv(self, name: str, path: str):
        pass

    def register_table(self, name: str, df: DataFrame):
        pass

    def deregister_table(self, name: str):
        pass
