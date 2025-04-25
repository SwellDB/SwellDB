# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableSequence
from langchain_community.utilities import GoogleSerperAPIWrapper

import os

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

llm = ChatOpenAI(api_key=os.getenv("OPENAI_API_KEY"), temperature=0, model="gpt-4o")


def generate_table_with_search(content_description: str, schema: str):
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a world class information retrieval master"),
            ("user", "{input}"),
        ]
    )

    search_query = llm(
        f"Generate a search engine query to retrieve the following info: {content_description}"
    ).content

    search = GoogleSerperAPIWrapper()

    search_results = search.run(search_query)

    chain: RunnableSequence = prompt | llm

    return chain.invoke({"input": text}).contents


def generate_table(text: str):
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a world class information retrieval master"),
            ("user", "{input}"),
        ]
    )

    chain: RunnableSequence = prompt | llm

    return chain.invoke({"input": text}).contents
