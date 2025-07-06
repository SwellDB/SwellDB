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

from swelldb.common.text import Splitter
from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.prompt.prompt_utils import create_table_prompt
from swelldb.search.utils import crawl
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.util.config_parser import ConfigParser
from swelldb.util.globals import Globals
from swelldb.table_plan.meta import SwellDBMeta

import logging


class SearchEngineTable(PhysicalTable):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        meta: SwellDBMeta,
        llm: AbstractLLM,
    ):
        super().__init__(
            execution_engine=execution_engine,
            llm=llm,
            logical_table=logical_table,
            child_table=child_table,
            operator_name="search_engine_table",
            layout=meta.get_layout(),
            base_columns=meta.get_base_columns(),
            chunk_size=meta.get_chunk_size(),
        )

        self._execution_engine = execution_engine
        self._meta = meta
        self._llm = llm

        # Set up Jinja environment
        current_dir = os.path.dirname(os.path.abspath(__file__))
        prompts_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(current_dir))),
            "table_plan",
            "prompts",
        )
        self._env = Environment(loader=FileSystemLoader(prompts_dir))

        # Load config
        serper_api_key = meta.get_serper_api_key()
        if serper_api_key:
            self._serper_api_key = serper_api_key
        elif os.getenv("SERPER_API_KEY"):
            self._serper_api_key = os.getenv("SERPER_API_KEY")
        else:
            self._serper_api_key = ConfigParser.get_config(
                Globals.GOOGLE_SERPER_API_KEY
            )

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        logging.info("Searching on the internet")

        data: List = list()

        if input_table:
            if self._base_columns:
                data = input_table.select(self._base_columns).to_pylist()
            else:
                data = input_table.to_pylist()

        links: List[str] = self._meta.get_links()

        if not links:
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

            for query in search_queries:
                logging.info(f"Issuing query: {query}")
                results: dict = search.results(query)
                parsed_results: str = str(results["organic"])
                search_results = search_results + "\n" + parsed_results

                for result in results["organic"]:
                    link = result["link"]
                    links.append(link)

        crawl_pages: bool = False

        if crawl_pages:
            clean_results = ""

            for link in links:
                logging.info(f"Crawling link: {link}")
                content = crawl(link)
                if content:
                    clean_results += "\n" + content

            splitter: Splitter = Splitter()
            prompts = []
            for chunk in splitter.split(clean_results):
                prompt: str = create_table_prompt(
                    table_description=self._logical_table.get_prompt(),
                    table_schema=self._logical_table.get_schema().get_attribute_names(),
                    data=f"Original data: {data}\nSearch results: {chunk}",
                    layout=self._layout,
                )

                prompts.append(prompt)
        else:
            prompt: str = create_table_prompt(
                table_description=self._logical_table.get_prompt(),
                table_schema=self._logical_table.get_schema().get_attribute_names(),
                data=f"Original data: {data}\nSearch results: {search_results}",
                layout=self._layout,
            )

            prompts = [prompt]

        logging.info(f"Table prompt: {prompt}")

        return prompts

    @override
    def __str__(self):
        return f"SearchEngineTable[schema={self._logical_table.get_schema().get_attribute_names()}"