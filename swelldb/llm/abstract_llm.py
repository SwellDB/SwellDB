# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import json

from langchain_core.language_models import BaseChatModel


class AbstractLLM:
    def __init__(self, llm: BaseChatModel):
        self.llm: BaseChatModel = llm

        # Stats
        self.input_tokens = 0
        self.output_tokens = 0

    def call(self, prompt: str) -> str:
        r = self.llm.invoke(prompt)
        stats = r.usage_metadata

        self.input_tokens += stats["input_tokens"]
        self.output_tokens += stats["output_tokens"]

        r = r.content

        if "```json" in r:
            r = r.split("```json")[1].split("```")[0]
        else:
            r = r

        # For Deepseek responses
        if "</think>" in r:
            r = r.split("</think>")[1]

        return r

    def needs_search(self, content: str) -> bool:
        # Ask if further information is needed
        ask_prompt: str = f"""\
        I need to construct a table that contains information about the following content:

        content: {content}

        Your response should be up-to-date. Does your data suffice to do that?
        
        If no, respond 0. If yes, respond 1.
        """

        ans: str = self.llm(ask_prompt).content

        needs_search: bool = int(ans) == 0

        return needs_search
