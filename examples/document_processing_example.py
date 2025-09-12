#!/usr/bin/env python3
"""
Example demonstrating PDF/Document processing with SwellDB DocumentTable.

This example shows how to:
1. Load documents (PDFs, DOCX, TXT)
2. Extract structured data using LLMs
3. Query the results with SQL
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from swelldb.table_plan.table.physical.document_table import DocumentTable
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.swelldb_schema import SwellDBSchema
from swelldb.table_plan.swelldb_attribute import SwellDBAttribute
from swelldb.table_plan.meta import SwellDBMeta
from swelldb.engine.datafusion_processor import DataFusionEngine
from swelldb.llm.openai_llm import OpenAILLM


def main():
    """Demonstrate document processing with SwellDB."""
    
    # Initialize components
    engine = DataFusionEngine()
    # llm = OllamaLLM("gpt-oss")
    llm = OpenAILLM("gpt-4o")

    # Define schema for extracted data
    schema = SwellDBSchema([
        SwellDBAttribute("job_title", "string", "the job title"),
        SwellDBAttribute("dates", "string", "the dates he worked there"),
        SwellDBAttribute("description", "string", "the job description"),
    ])
    
    # Create logical table
    logical_table = LogicalTable(
        name="document_analysis",
        prompt="Candidate's resume",
        schema=schema
    )
    
    # Configure metadata with document paths
    meta = SwellDBMeta()
    meta.set_documents([
        # Add your document paths here - can be a folder or individual files
        # "/path/to/document2.docx",  # Or individual files
        # "/path/to/document3.txt"
    ])
    meta.set_base_columns(["title"])
    meta.set_chunk_size(10)
    
    # Create document table using DOCUMENT mode
    doc_table = DocumentTable(
        execution_engine=engine,
        logical_table=logical_table,
        child_table=None,
        meta=meta,
        llm=llm
    )
    
    print("DocumentTable created successfully!")
    print(f"Schema: {schema.get_attribute_names()}")
    
    # Note: To actually process documents, you need to:
    # 1. Add real document paths to meta.set_documents()
    # 2. Ensure the PDF/document processing dependencies are installed
    # 3. Call doc_table.materialize() to extract structured data
    
    # This would process the documents if paths were provided
    result = doc_table.materialize()
    print(result.to_pandas())
    print(f"Processed {result.num_rows} rows from documents")
    print("Add document paths to meta.set_documents() to process actual documents")



if __name__ == "__main__":
    main()
