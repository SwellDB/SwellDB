# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List, Dict

from pyarrow.lib import DataType

import pyarrow as pa

from swelldb.table_plan.swelldb_attribute import SwellDBAttribute


class SwellDBSchema:
    def __init__(self, attributes: List[SwellDBAttribute]):
        self._attribute_list: List[SwellDBAttribute] = attributes
        self._attributes: Dict[str, SwellDBAttribute] = dict(
            [(attr.get_name(), attr) for attr in attributes]
        )

    def get_attributes(self) -> List[SwellDBAttribute]:
        return self._attribute_list

    def get_attribute(self, name: str) -> SwellDBAttribute:
        return self._attributes[name]

    def get_attribute_names(self) -> List[str]:
        return [d.get_name() for d in self._attribute_list]

    def to_arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                (attr.get_name(), attr.get_data_type(), attr.get_description())
                for attr in self._attribute_list
            ]
        )

    @staticmethod
    def from_string(schema: str) -> "SwellDBSchema":
        attributes = []
        for attr in schema.split(","):
            attr_name, attr_type = attr.strip().split(" ")
            dt = pa.from_numpy_dtype(attr_type.strip())
            attributes.append(SwellDBAttribute(attr_name.strip(), dt))
        return SwellDBSchema(attributes)


class SwellDBSchemaBuilder:
    def __init__(self):
        self._attributes: Dict[str, SwellDBAttribute] = dict()

    def add_attribute(self, name: str, data_type: DataType, description: str):
        self._attributes[name] = SwellDBAttribute(
            name=name, dtype=data_type, description=description
        )

        return self

    def build(self):
        return SwellDBSchema(self._attributes.values())
