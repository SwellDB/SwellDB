# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os

from langchain_openai import ChatOpenAI

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.util.config_parser import ConfigParser
from swelldb.util.globals import Globals


class OpenAILLM(AbstractLLM):
    def __init__(self, api_key: str = None, temperature: int = 0) -> None:
        self.api_key: str = ""

        if api_key:
            self.api_key = api_key
        elif os.getenv(Globals.OPENAI_API_KEY):
            self.api_key = os.getenv(Globals.OPENAI_API_KEY)

        llm = ChatOpenAI(
            api_key=self.api_key,
            temperature=temperature,
            model="gpt-4o",
        )
        super().__init__(llm=llm)
