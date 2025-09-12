# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

"""
Example demonstrating image processing with SwellDB ImageTable.
This example shows how to use ImageTable to process images from a folder
and extract information using LLM vision capabilities.
"""

import os

import pandas as pd

from swelldb import SwellDB, OpenAILLM
from swelldb.swelldb import Mode

def image_table_example():
    # Initialize SwellDB with OpenAI LLM (requires GPT-4o or similar vision model)
    swelldb = SwellDB(
        llm=OpenAILLM(api_key=os.environ.get("OPENAI_API_KEY"), model="gpt-4o", temperature=0.8)
    )
    
    # Example: Process images from a folder to extract information
    # Replace with your actual image folder path
    image_folder_path = ".."
    
    # Note: The IMAGE mode requires at least one image path to be added
    # If no images are provided, it will raise a ValueError
    
    # Create a table that processes images
    image_table = (
        swelldb.table_builder()
        .set_table_name("image_analysis")
        .set_content("Extract information from the images. The animal type should be in lower case.")
        .set_schema("image_name str, description str, animal_type str, file_name str, wiki_link str")
        .set_table_gen_mode(Mode.IMAGE)
        .add_images(image_folder_path)  # Add the folder containing images
    ).build()

    # Show full DataFrame without truncation
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.width', None)  # Don't wrap lines
    pd.set_option('display.max_colwidth', None)  # Show full content in cells

    print(image_table.materialize().to_pandas())

if __name__ == "__main__":
    image_table_example()
