# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import Union, List
import pyarrow as pa

from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.mode import Mode


class SwellDBMeta:
    def __init__(self):
        self._links: List[str] = []
        self._images: List[str] = []
        self._data: pa.Table = None
        self._base_columns: List[str] = None
        self._schema: Union[SwellDBSchema, str] = None
        self._content: str = None
        self._table_name: str = None
        self._table_gen_mode: Mode = Mode.LLM
        self._operators: List[type] = []
        self._chunk_size: int = 20
        self._layout: Layout = Layout.ROW()
        self._serper_api_key: str = None

    def set_links(self, links: List[str]) -> "SwellDBMeta":
        self._links = links
        return self

    def set_images(self, images: List[str]) -> "SwellDBMeta":
        self._images = images
        return self

    def set_data(self, data: pa.Table) -> "SwellDBMeta":
        self._data = data
        return self

    def set_base_columns(self, base_columns: List[str]) -> "SwellDBMeta":
        self._base_columns = base_columns
        return self

    def set_schema(self, schema: Union[SwellDBSchema, str]) -> "SwellDBMeta":
        self._schema = schema
        return self

    def set_content(self, content: str) -> "SwellDBMeta":
        self._content = content
        return self

    def set_table_name(self, name: str) -> "SwellDBMeta":
        self._table_name = name
        return self

    def set_table_gen_mode(self, mode: Mode) -> "SwellDBMeta":
        self._table_gen_mode = mode
        return self

    def set_operators(self, operators: List[type]) -> "SwellDBMeta":
        op_set = set()
        for operator in operators:
            if operator in op_set:
                raise ValueError(
                    f"Duplicate operator found: {operator}. Each operator should be included only once."
                )
            op_set.add(operator)
        self._operators = operators
        return self

    def set_chunk_size(self, chunk_size: int) -> "SwellDBMeta":
        self._chunk_size = chunk_size
        return self

    def set_layout(self, layout: Layout) -> "SwellDBMeta":
        self._layout = layout
        return self

    def set_serper_api_key(self, serper_api_key: str) -> "SwellDBMeta":
        self._serper_api_key = serper_api_key
        return self

    # Getters
    def get_links(self) -> List[str]:
        return self._links

    def get_images(self) -> List[str]:
        return self._images

    def get_data(self) -> pa.Table:
        return self._data

    def get_base_columns(self) -> List[str]:
        return self._base_columns

    def get_schema(self) -> Union[SwellDBSchema, str]:
        return self._schema

    def get_content(self) -> str:
        return self._content

    def get_table_name(self) -> str:
        return self._table_name

    def get_table_gen_mode(self) -> Mode:
        return self._table_gen_mode

    def get_operators(self) -> List[type]:
        return self._operators

    def get_chunk_size(self) -> int:
        return self._chunk_size

    def get_layout(self) -> Layout:
        return self._layout

    def get_serper_api_key(self) -> str:
        return self._serper_api_key

    def add_link(self, link: str) -> "SwellDBMeta":
        if link not in self._links:
            self._links.append(link)
        return self

    def add_image(self, image_path: str) -> "SwellDBMeta":
        if image_path not in self._images:
            self._images.append(image_path)
        return self