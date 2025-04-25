# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
from typing import List, Dict

from jinja2 import Template
from overrides import override, overrides
from pyarrow import Table

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.engine.execution_engine import ExecutionEngine


class DocumentTable(PhysicalTable):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        base_columns: List[str],
        llm: AbstractLLM,
        layout: Layout = Layout.ROW(),
    ):
        super().__init__(
            logical_table=logical_table,
            child_table=child_table,
            layout=layout,
            operator_name="document_table",
            llm=llm,
            base_columns=base_columns,
            execution_engine=execution_engine,
        )

        self._execution_engine = execution_engine

    @override
    def materialize(self, partitions=1) -> Table:
        tables = self._execution_engine.get_tables()

        if self._query:
            sql_query = self._query
        else:
            sql_query = (
                self._llm.call(
                    f"""
            You have access to the following table schemas:
            f{tables}
            
            Generate a SQL query to extract the data from the tables. Your target schema is the following:
            {self._logical_table.get_schema().get_attribute_names()}
            
            Use aliases if needed. Return only the SQL query as a python-compatibel text format.
            """
                )
                .replace("sql", "")
                .replace("```", "")
            )

        return self._execution_engine.sql(sql_query).to_arrow_table()

    @overrides
    def get_columns_prompt(logical_table: LogicalTable, tables: Dict[str, str]) -> str:
        # Get the directory of the current file
        current_dir = os.path.dirname(__file__)

        # Construct the relative path to the target file or directory
        prompt_file_path = os.path.join(
            current_dir, "../../prompts", f"dataset_table_columns_prompt.jinja"
        )

        tables_str = ""

        for tbl in tables:
            tables_str += f"Table name:{tbl}: Schema: {tables[tbl]}\n"

        # Read the file and render the template
        with open(prompt_file_path, "r") as file:
            template = Template(file.read())
            prompt = template.render(
                content=logical_table.get_prompt(),
                schema=logical_table.get_schema().get_attribute_names(),
                tables=tables_str,
            )

            return prompt

    @override
    def __str__(self):
        return f'DatasetTable[schema={self._logical_table.get_schema().get_attribute_names()}"]'
