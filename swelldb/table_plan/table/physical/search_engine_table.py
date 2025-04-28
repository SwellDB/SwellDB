# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List
import os
import pyarrow as pa
from langchain_community.utilities import GoogleSerperAPIWrapper
from overrides import override
from jinja2 import Environment, FileSystemLoader

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.prompt.prompt_utils import create_table_prompt
from swelldb.table_plan.layout import Layout
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.engine.execution_engine import ExecutionEngine

import logging

from swelldb.util.config_parser import ConfigParser
from swelldb.util.globals import Globals


class SearchEngineTable(PhysicalTable):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        base_columns: List[str],
        llm: AbstractLLM,
        layout: Layout = Layout.ROW(),
        serper_api_key: str = None,
    ):
        super().__init__(
            execution_engine=execution_engine,
            llm=llm,
            logical_table=logical_table,
            child_table=child_table,
            operator_name="search_engine_table",
        )

        self._execution_engine = execution_engine
        self._base_columns = base_columns
        self._llm = llm
        self._layout = layout
        self._serper_api_key = serper_api_key

        # Set up Jinja environment
        current_dir = os.path.dirname(os.path.abspath(__file__))
        prompts_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(current_dir))),
            "table_plan",
            "prompts",
        )
        self._env = Environment(loader=FileSystemLoader(prompts_dir))

        # Load config

        if serper_api_key:
            self._serper_api_key = serper_api_key
        elif os.getenv("SERPER_API_KEY"):
            self._serper_api_key = os.getenv("SERPER_API_KEY")
        else:
            self._serper_api_key = ConfigParser.get_config(
                Globals.GOOGLE_SERPER_API_KEY
            )

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        # Search
        logging.info("Searching on the internet")

        data: List = list()

        if input_table:
            if self._base_columns:
                data = input_table.select(self._base_columns).to_pylist()
            else:
                data = input_table.to_pylist()

        # Load and render the Jinja template
        template = self._env.get_template("search_engine_prompt.jinja")
        search_query_prompt = template.render(
            prompt=self._logical_table.get_prompt(),
            sql_query=self._logical_table._sql_query,
            schema=self._logical_table.get_schema().get_attribute_names(),
            data=data,
        )

        search_queries: list[str] = self._llm.call(search_query_prompt).split("\n")

        logging.info(f"Search queries: {search_queries}")

        search: GoogleSerperAPIWrapper = GoogleSerperAPIWrapper(
            serper_api_key=self._serper_api_key
        )

        search_results: str = ""

        links = []

        for query in search_queries:
            logging.info(f"Issuing query: {query}")
            results: dict = search.results(query)
            parsed_results: str = str(results["organic"])
            search_results = search_results + "\n" + parsed_results

            for result in results["organic"]:
                link = result["link"]
                links.append(link)

        prompt: str = create_table_prompt(
            table_description=self._logical_table.get_prompt(),
            table_schema=self._logical_table.get_schema().get_attribute_names(),
            data=f"Original data: {data}\nSearch results: {search_results}",
            layout=self._layout,
        )

        logging.info(f"Table prompt: {prompt}")

        return [prompt]

    @override
    def __str__(self):
        return f"SearchEngineTable[schema={self._logical_table.get_schema().get_attribute_names()}"
