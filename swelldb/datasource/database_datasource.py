# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import Dict

from swelldb.datasource.datasource import Datasource

import pyarrow as pa


class DatabaseDatasource(Datasource):
    def sql(self, query: str) -> pa.Table:
        raise NotImplementedError()

    def schema(self) -> Dict[str, str]:
        raise NotImplementedError()
