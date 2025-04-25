# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
from typing import Dict

from jinja2 import Template

from swelldb.table_plan.table.logical.logical_table import LogicalTable


@DeprecationWarning
def get_main_prompt(table: LogicalTable) -> str:
    prompt = f"""
Construct a table that contains the following content and schema:

content: {table.get_prompt()}
schema:  {table.get_schema()}
datasets: None

Your response should consist of the following JSON:

response structure: {plan_structure_prompt()}
    """

    return prompt


@DeprecationWarning
def plan_structure_prompt() -> str:
    structure = """
{    
    "dataset_columns": The columns that can be generated from any of the dataset. In dictionary format,
    of the form {dataset_name: {requested_column_name: original_column_name}}

    "missing_columns": The names of the columns of the requested schema that cannot be extracted from any of the dataset. 

    "llm_columns": The names of the columns that cannot be obtained from any of the given datasets or databases,
    but they are included in your knowledge.

    "search_engine_columns": The columns that you can generate only if you are provided with external data.

    "datasets": A list of the datasets you will use from the ones provided in the prompt, if any. Else return
    an empty list.

    "db_query": Generate a SQL query that extracts the data from the given database, if any. Otherwise return ''. 
    Use the appropriate aliases to comply with the requested schema. Your SQL query should also comply with the 
    schema of the given database.

    "linking_column": The most representative column for that entity of the given schema.
     Return this as a tuple of the form [column_name, dataset_name]. Use the column name according to the user-defined schema."
}
"""

    return structure


@DeprecationWarning
def get_last_line() -> str:
    return "\nReturn only the JSON response and no additional text. Do not start with ```json."


def get_local_tables_prompt(
    logical_table: LogicalTable, table_schema_dict: Dict[str, str]
):
    """
    Generate the prompt for the columns that can be retrieved from the local tables
    :param logical_table: The logical table
    :return: The prompt
    """

    # Get the directory of the current file
    current_dir = os.path.dirname(__file__)

    # Construct the relative path to the target file or directory
    prompt_file_path = os.path.join(
        current_dir, "prompts", "table_columns_prompt.jinja"
    )

    # Read the file and render the template
    with open(prompt_file_path, "r") as file:
        template = Template(file.read())
        prompt = template.render(
            content=logical_table.get_prompt(),
            schema=logical_table.get_schema().get_attribute_names(),
            tables=str(table_schema_dict),
        )

        return prompt


def get_llm_columns_prompt(logical_table: LogicalTable):
    """
    Generate the prompt for the LLM columns
    :param logical_table: The logical table
    :return: The prompt
    """

    # Get the directory of the current file
    current_dir = os.path.dirname(__file__)

    # Construct the relative path to the target file or directory
    prompt_file_path = os.path.join(
        current_dir, "prompts", "llm_table_columns_prompt.jinja"
    )

    # Read the file and render the template
    with open(prompt_file_path, "r") as file:
        template = Template(file.read())
        prompt = template.render(
            content=logical_table.get_prompt(),
            schema=logical_table.get_schema().get_attribute_names(),
        )

        return prompt
