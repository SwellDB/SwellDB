# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import pyarrow as pa
from pyarrow.lib import DataType


class SwellDBAttribute:
    def __init__(self, name: str, dtype: DataType, description: str = None):
        self._name: str = name
        self._dtype: DataType = dtype
        self._description = description

    def get_name(self) -> str:
        return self._name

    def get_data_type(self) -> DataType:
        return self._dtype

    def get_description(self) -> str:
        return self._description
