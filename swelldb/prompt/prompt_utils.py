# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from langchain.prompts import ChatPromptTemplate
from langchain_core.messages import BaseMessage
import os
from jinja2 import Environment, FileSystemLoader

from swelldb.table_plan.layout import Layout

column_format_examples = """
content: A list of movies
schema: {{'movie_name': 'the movie name', 'description': 'generate a description of the movie'}}
tuples: [['movie_1_name'], ['movie_2_name'], ['movie_3_name']]

"columns": {
  "movie_name": ["LOTR", "Harry Potter"],
  "description": [
    "The Lord of the Rings (LOTR) is an epic fantasy saga...",
    "Harry Potter is a fantasy film series..."
  ]
}
"""

row_format_examples = """
content: A list of movies
schema: {{'movie_name', 'the movie name', 'rating': 'the IMDB rating'}}

{
  "rows": [
    ["The Empire Strikes Back", 8.7],
    ["Blade Runner", 8.1],
    ["Back to the Future", 8.5]
  ]
}

content: A list of punk rock bands from the 80s
schema: {{'band_name', '', 'country': 'the country of origin'}}
{
  "rows": [
    ["The Ramones", "USA"],
    ["The Clash", "UK"],
    ["Dead Kennedys", "USA"]
  ]
}
"""

column_format_examples_with_data = """
content: None
schema: {superhero_name: the name, production_company: the comic production company}
tuples: {superman, spiderman, captain america}

"columns": {
  "superhero_name": ["Superman", "Spider-Man", "Captain America"],
  "production_company": ["DC Comics", "Marvel Comics", "Marvel Comics"]
}
"""

row_format_examples_with_data = """
content: None
schema: {superhero_name: the name, production_company: the comic production company}
tuples: {superman, spiderman, captain america}

"rows": [
    ["Superman", "DC Comics"],
    ["Spider-Man", "Marvel Comics"],
    ["Captain America", "Marvel Comics"]
  ]
"""


def generate_table_prompt(
    table_description: str, table_schema: dict[str, str]
) -> list[BaseMessage]:
    prompt: str = """\
    For the following table description, generate a table that contains rows with the following attributes:
    
    attributes: {attributes}
    
    description: {table_description}
    
    The format should be a key named 'rows', which maps to a list of rows.
    
    Produce the output in JSON format, such as it can by directly parsed in Python. Do not include ```json.
    """

    prompt_template: ChatPromptTemplate = ChatPromptTemplate.from_template(prompt)

    messages: list[BaseMessage] = prompt_template.format_messages(
        table_description=table_description, attributes=table_schema
    )

    return messages


def create_table_prompt(
    table_description: str, table_schema: dict[str, str], data: str, layout: Layout
):
    # Set up Jinja environment
    current_dir = os.path.dirname(os.path.abspath(__file__))
    prompts_dir = os.path.join(
        os.path.dirname(os.path.dirname(current_dir)),
        "swelldb",
        "table_plan",
        "prompts",
    )
    env = Environment(loader=FileSystemLoader(prompts_dir))

    # Load and render the template
    template = env.get_template("table_prompt.jinja")
    table_gen_prompt = template.render(
        table_description=table_description,
        table_schema=table_schema,
        data=data,
        layout=layout.get_name(),
    )

    return table_gen_prompt
