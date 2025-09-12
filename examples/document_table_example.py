#!/usr/bin/env python3
"""
Example demonstrating document processing with SwellDB DocumentTable using DOCUMENT mode.

This example shows how to:
1. Use the new DOCUMENT mode for automatic DocumentTable creation
2. Process documents from a folder using add_documents()
3. Extract structured data using LLMs
4. Query the results with SQL
"""

import os
from swelldb import SwellDB, OpenAILLM
from swelldb.swelldb import Mode

def main():
    """Demonstrate document processing with SwellDB DocumentTable using DOCUMENT mode."""
    
    # Initialize SwellDB with OpenAI LLM (requires GPT-4o or similar)
    swelldb = SwellDB(
        llm=OpenAILLM(api_key=os.environ.get("OPENAI_API_KEY"), model="gpt-4o")
    )
    
    print("SwellDB initialized with OpenAI LLM")
    
    # Example: Process documents from a folder to extract information
    # Replace with your actual document folder path
    document_folder_path = ""
    
    # Verify the folder exists and contains documents
    if os.path.exists(document_folder_path):
        # List document files in the folder
        doc_extensions = ['.pdf', '.docx', '.doc', '.txt', '.rtf']
        doc_files = []
        
        for ext in doc_extensions:
            doc_files.extend([f for f in os.listdir(document_folder_path) 
                           if f.lower().endswith(ext)])
        
        print(f"Found {len(doc_files)} document files in {document_folder_path}")
        if doc_files:
            print("Sample documents:", doc_files[:5])
        else:
            print("No document files found. Please check the folder path.")
    else:
        print(f"Folder {document_folder_path} does not exist. Please update the path.")
    
    # Create a table that processes documents using DOCUMENT mode
    document_table = (
        swelldb.table_builder()
        .set_table_name("document_analysis")
        .set_content("Extract information from the documents. Extract job titles, dates, and descriptions from resumes.")
        .set_schema("job_title str, dates str, description str, file_name str")
        .set_table_gen_mode(Mode.DOCUMENT)
        .add_documents(document_folder_path)  # Add the folder containing documents
    ).build()
    
    print("DocumentTable created successfully!")
    print(f"Table name: {document_table._logical_table.get_name()}")
    print(f"Schema: {document_table._logical_table.get_schema().get_attribute_names()}")
    print(f"Content: {document_table._logical_table.get_prompt()}")
    
    # Generate prompts for all documents
    prompts = document_table.get_prompts(None)
    
    print(f"\nGenerated {len(prompts)} prompts for documents")
    
    # Show a sample prompt (first 500 characters to avoid overwhelming output)
    if prompts:
        print("\nSample prompt (first 500 characters):")
        print("-" * 50)
        print(prompts[0][:500] + "..." if len(prompts[0]) > 500 else prompts[0])
        
        # Show the structure of the prompt
        print("\nPrompt structure:")
        print(f"- Total length: {len(prompts[0])} characters")
        print(f"- Contains document content: {'Document content:' in prompts[0]}")
        print(f"- Contains schema: {len(document_table._logical_table.get_schema().get_attribute_names())} columns")
    
    # Process documents and create the final table
    print("\nProcessing documents through LLM...")
    print("This may take some time depending on the number of documents and LLM response time.")
    
    try:
        # Materialize the table - this processes all documents
        result_table = document_table.materialize()
        
        print("\nDocument processing completed successfully!")
        print(f"Result table shape: {result_table.shape}")
        print(f"Result table schema: {result_table.schema}")
        
        # Convert to pandas DataFrame for better display
        df = result_table.to_pandas()
        print("\nFinal Results:")
        print("=" * 50)
        print(df)
        
        # Show some statistics
        print("\nSummary:")
        print(f"Total documents processed: {len(df)}")
        
        if 'job_title' in df.columns:
            print(f"\nJob titles found:")
            print(df['job_title'].value_counts())
        
        if 'description' in df.columns:
            print(f"\nAverage description length: {df['description'].str.len().mean():.1f} characters")
            
    except Exception as e:
        print(f"Error during materialization: {e}")
        print("This might be due to LLM API issues or document processing errors.")

if __name__ == "__main__":
    main()

