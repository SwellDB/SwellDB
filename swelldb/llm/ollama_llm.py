# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from langchain_ollama import ChatOllama

from swelldb.llm.abstract_llm import AbstractLLM


class OllamaLLM(AbstractLLM):
    def __init__(self, model):
        llm = ChatOllama(model=model, temperature=0)
        super().__init__(llm=llm)
