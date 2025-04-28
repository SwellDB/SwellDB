# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List, Dict
import os
import pyarrow as pa
from jinja2 import Environment, FileSystemLoader, Template
from overrides import overrides

from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.prompt.prompt_utils import create_table_prompt
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable

import logging


class LLMTable(PhysicalTable):
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
            execution_engine=execution_engine,
            logical_table=logical_table,
            child_table=child_table,
            layout=layout,
            operator_name="llm_table",
            llm=llm,
            base_columns=base_columns,
        )

        # Set up Jinja environment
        current_dir = os.path.dirname(os.path.abspath(__file__))
        prompts_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(current_dir))),
            "table_plan",
            "prompts",
        )
        self._env = Environment(loader=FileSystemLoader(prompts_dir))

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        logging.info("Generating LLM Table")

        data: List = list()

        if input_table:
            if self._base_columns:
                data = input_table.select(self._base_columns).to_pylist()
            else:
                data = input_table.to_pylist()

        schema: SwellDBSchema = self._logical_table.get_schema()

        prompt: str = create_table_prompt(
            table_description=self._logical_table.get_prompt(),
            table_schema=schema.get_attribute_names(),
            data=data,
            layout=self._layout,
        )

        return [prompt]

    @staticmethod
    def get_columns_prompt(logical_table: LogicalTable, tables: Dict[str, str]) -> str:
        # Get the directory of the current file
        current_dir = os.path.dirname(__file__)

        # Construct the relative path to the target file or directory
        prompt_file_path = os.path.join(
            current_dir, "../../prompts", f"llm_table_columns_prompt.jinja"
        )

        # Read the file and render the template
        with open(prompt_file_path, "r") as file:
            template = Template(file.read())
            prompt = template.render(
                content=logical_table.get_prompt(),
                schema=logical_table.get_schema().get_attribute_names(),
            )

            return prompt

    def __str__(self):
        return (
            f"LLMTable[schema={self._logical_table.get_schema().get_attribute_names()}"
        )
