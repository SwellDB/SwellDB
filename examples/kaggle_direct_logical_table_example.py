# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

"""
Example of creating KaggleDatasetTable directly from a logical table.
"""

from swelldb.llm.openai_llm import OpenAILLM
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.kaggle_dataset_table import KaggleDatasetTable
from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.table_plan.meta import TableConfig

import json

def main():
    """Example of creating KaggleDatasetTable directly from logical table."""
    print("🔍 KaggleDatasetTable - Direct Creation from Logical Table")
    print("=" * 60)
    
    # Initialize LLM
    llm = OpenAILLM(temperature=0, model="gpt-4o")
    
    # Create schema for the table
    schema = SwellDBSchema.from_string(
        "company_name str, revenue_millions str"
    )
    
    # Create logical table
    logical_table = LogicalTable(
        name="fortune_500_companies",
        prompt="A table that contains the fortune 500 companies from 2012",
        schema=schema
    )
    
    # Create table configuration
    meta = TableConfig()
    meta.set_table_name("fortune_500_companies")
    meta.set_content(logical_table.get_prompt())
    meta.set_schema(schema)

    kaggle_table = KaggleDatasetTable(
        logical_table=logical_table,
        meta=meta,
        llm=llm
    )

    result_table = kaggle_table.materialize()

    print(result_table.to_pandas())


if __name__ == "__main__":
    main()