#!/usr/bin/env python3
"""
RawTextTable Example

This example demonstrates how to use SwellDB's RawTextTable to process text files
and extract structured data from them using LLM processing.

The RawTextTable works with text files as input, similar to how ImageTable works with image files.
"""

import logging
import os
from swelldb import SwellDB
from swelldb.llm.openai_llm import OpenAILLM
from swelldb.table_plan.mode import Mode

# Set up logging to see the processing
logging.basicConfig(level=logging.INFO)

def main():
    # Initialize SwellDB with OpenAI LLM
    llm = OpenAILLM("gpt-4o")
    swell = SwellDB(llm=llm)
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sample_files_dir = os.path.join(script_dir, "sample_text_files")
    
    # Sample text files about companies
    text_files = [
        os.path.join(sample_files_dir, "apple.txt"),
        os.path.join(sample_files_dir, "microsoft.txt"),
        os.path.join(sample_files_dir, "google.txt"),
        os.path.join(sample_files_dir, "amazon.txt"),
    ]
    
    # Verify files exist
    for file_path in text_files:
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} does not exist")
    
    # Create a table using RawTextTable mode
    table = swell.table_builder() \
        .set_content("Create a table that contains information about companies.") \
        .set_schema("company_name str, founder str, founded_year int, headquarters str, revenue_2023 str, main_products str") \
        .set_table_gen_mode(Mode.RAWTEXT) \
        .set_chunk_size(5)
    
    # Add text files
    for text_file in text_files:
        table.add_text_files(text_file)
    
    # Build the table
    table = table.build()
    
    # Materialize the table to get the results
    result = table.materialize()
    
    print("RawTextTable Results:")
    print("=" * 50)
    print(result.to_pandas())
    
    # Show the schema
    print(f"\nSchema: {result.schema}")
    print(f"Number of rows: {len(result)}")

if __name__ == "__main__":
    main()
