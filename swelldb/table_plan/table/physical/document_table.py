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
from swelldb.table_plan.meta import SwellDBMeta
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.common.document_loader import DocumentLoader
from swelldb.common.text import Splitter
from swelldb.prompt.prompt_utils import create_table_prompt


class DocumentTable(PhysicalTable):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        meta: SwellDBMeta,
        llm: AbstractLLM,
    ):
        super().__init__(
            logical_table=logical_table,
            child_table=child_table,
            layout=meta.get_layout(),
            operator_name="document_table",
            llm=llm,
            base_columns=meta.get_base_columns(),
            execution_engine=execution_engine,
            chunk_size=meta.get_chunk_size(),
        )

        self._execution_engine = execution_engine
        self._meta = meta
        self._document_loader = DocumentLoader()
        self._splitter = Splitter()

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        """Generate prompts from document content and input data."""
        logging.info("Processing documents for DocumentTable")
        
        # Get document paths from meta or links
        document_paths = self._meta.get_links() if self._meta.get_links() else []
        
        if not document_paths:
            logging.warning("No document paths provided in meta.links")
            return []
        
        all_document_text = ""
        for doc_path in document_paths:
            try:
                doc_text = self._document_loader.load_document(doc_path)
                all_document_text += f"\n\n--- Document: {doc_path} ---\n{doc_text}"
            except Exception as e:
                logging.error(f"Failed to load document {doc_path}: {e}")
                continue
        
        if not all_document_text:
            logging.warning("No document content extracted")
            return []
        
        # Split document into chunks if it's too large
        text_chunks = self._splitter.split(all_document_text)
        
        prompts = []
        for chunk in text_chunks:
            prompt = create_table_prompt(
                table_description=self._logical_table.get_prompt(),
                table_schema=self._logical_table.get_schema().get_attribute_names(),
                data=f"Document content:\n{chunk}",
                layout=self._layout,
            )
            prompts.append(prompt)
        
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
        return f'DocumentTable[schema={self._logical_table.get_schema().get_attribute_names()}"]'
