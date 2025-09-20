# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
from typing import List, Dict
import logging

from jinja2 import Template
from overrides import override, overrides
import pyarrow as pa

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.meta import TableConfig
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.prompt.prompt_utils import create_table_prompt


class RawTextTable(PhysicalTable):
    def __init__(
        self,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        meta: TableConfig,
        llm: AbstractLLM,
    ):
        super().__init__(
            logical_table=logical_table,
            child_table=child_table,
            layout=meta.get_layout(),
            operator_name="rawtext_table",
            llm=llm,
            base_columns=meta.get_base_columns(),
            chunk_size=meta.get_chunk_size(),
        )

        self._meta = meta

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        """Generate prompts from text files - one prompt per file."""
        logging.info("Processing text files for RawTextTable")
        
        # Get text file paths from meta
        text_file_paths = self._meta.get_text_files() if self._meta.get_text_files() else []
        
        if not text_file_paths:
            logging.warning("No text file paths provided in meta.text_files")
            return []
        
        prompts = []
        for text_file_path in text_file_paths:
            try:
                with open(text_file_path, 'r', encoding='utf-8') as file:
                    file_content = file.read()
                    
                    # Create one prompt per file
                    # Convert schema to dictionary format expected by create_table_prompt
                    schema_dict = {
                        attr.get_name(): f"{attr.get_description() or attr.get_name()} (type: {attr.get_data_type()})"
                        for attr in self._logical_table.get_schema().get_attributes()
                    }
                    
                    prompt = create_table_prompt(
                        table_description=self._logical_table.get_prompt(),
                        table_schema=schema_dict,
                        data=f"Text file: {text_file_path}\nContent:\n{file_content}",
                        layout=self._layout,
                    )
                    prompts.append(prompt)
                    
            except Exception as e:
                logging.error(f"Failed to read text file {text_file_path}: {e}")
                continue
        
        logging.info(f"Generated {len(prompts)} prompts from {len(text_file_paths)} text files")
        return prompts

    @staticmethod 
    def get_columns_prompt(logical_table: LogicalTable, tables: Dict[str, str]) -> str:
        """Generate prompt for column planning (used by planner)."""
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
        return f'RawTextTable[schema={self._logical_table.get_schema().get_attribute_names()}]'
