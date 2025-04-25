# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List, Set, Dict

from swelldb.table_plan.swelldb_attribute import SwellDBAttribute
from swelldb.table_plan.swelldb_schema import SwellDBSchema


class LogicalTable:
    def __init__(
        self, name: str, prompt: str, schema: SwellDBSchema, sql_query: str = None
    ):
        self._name: str = name
        self._prompt: str = prompt
        self._schema: SwellDBSchema = schema
        self._sql_query: str = sql_query

    def get_name(self) -> str:
        return self._name

    def get_prompt(self) -> str:
        return self._prompt

    def get_schema(self) -> SwellDBSchema:
        return self._schema

    def split(self, schemas: List[SwellDBSchema]) -> List["LogicalTable"]:
        """
        Splits the table into multiple ones according to the input schemata. For each schema, a sub-table will be
        crated
        :param schemas: The input schemata
        :return: The list of the sub-tables
        """
        leftover_columns: Set[SwellDBAttribute] = set(self._schema.get_attributes())
        split: List[LogicalTable] = list()

        idx: int = 0
        for schema in schemas:
            for attribute in schema.get_attributes():
                leftover_columns.remove(attribute)

            split.append(
                LogicalTable(
                    name=f"{self._name}_{idx}", prompt=self._prompt, schema=schema
                )
            )
            idx += 1

        if leftover_columns:
            split.append(
                LogicalTable(
                    name=f"{self._name}_{idx}",
                    prompt=self._prompt,
                    schema=SwellDBSchema(leftover_columns),
                )
            )

        return split

    def __str__(self) -> str:
        return f"{self._name} - {self._schema} - {self._prompt}"
