# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os

from langchain_openai.chat_models.base import BaseChatOpenAI
from swelldb.llm.abstract_llm import AbstractLLM


class DeepseekOnlineLLM(AbstractLLM):
    def __init__(self):
        llm = BaseChatOpenAI(
            model="deepseek-chat",
            openai_api_key=os.getenv("DEEPSEEK_API_KEY"),
            openai_api_base="https://api.deepseek.com",
            max_tokens=1024,
        )
        super().__init__(llm=llm)
