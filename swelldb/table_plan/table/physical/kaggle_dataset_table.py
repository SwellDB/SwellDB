# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from typing import List, Dict, Any
import pyarrow as pa
import pandas as pd
import tempfile
import shutil
import os
import glob
import duckdb
from kagglesdk.datasets.types.dataset_api_service import ApiDataset
from overrides import override

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    KAGGLE_API_AVAILABLE = True
except ImportError:
    KAGGLE_API_AVAILABLE = False

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.table_plan.meta import TableConfig

import logging


class KaggleDatasetTable(PhysicalTable):
    def __init__(
        self,
        logical_table: LogicalTable,
        meta: TableConfig,
        llm: AbstractLLM,
    ):
        super().__init__(
            llm=llm,
            logical_table=logical_table,
            child_table=None,
            operator_name="kaggle_dataset_table",
            layout=meta.get_layout(),
            base_columns=meta.get_base_columns(),
            chunk_size=meta.get_chunk_size(),
        )

        self._meta = meta
        self._llm = llm
        
        # Initialize Kaggle API
        if not KAGGLE_API_AVAILABLE:
            logging.warning("Kaggle API not available. Install with: pip install kaggle")
            self._kaggle_api = None
        else:
            try:
                self._kaggle_api = KaggleApi()
                self._kaggle_api.authenticate()
                logging.info("Kaggle API authenticated successfully")
            except Exception as e:
                logging.warning(f"Failed to authenticate with Kaggle API: {e}")
                self._kaggle_api = None

    @override
    def get_prompts(self, input_table: pa.Table) -> List[str]:
        """Generate prompts for extracting data from Kaggle datasets."""
        # TODO: Implement Kaggle dataset search and prompt generation
        return []

    @override
    def materialize(self, partitions: int=1) -> pa.Table:
        """Materialize the table by finding, downloading, and loading the best Kaggle dataset."""
        
        if not self._kaggle_api:
            logging.error("Kaggle API not available")
            return pa.Table.from_pydict({})
        
        try:
            # Find the best matching dataset
            logging.info("Finding best matching dataset...")
            best_dataset = self._find_dataset()
            
            if not best_dataset:
                logging.warning("No suitable dataset found")
                return pa.Table.from_pydict({})
            
            dataset_ref = best_dataset.get('ref', '')
            if not dataset_ref:
                logging.error("Dataset reference not found")
                return pa.Table.from_pydict({})
            
            logging.info(f"Found dataset: {best_dataset.get('title', 'Unknown')}")
            
            # Download the dataset
            logging.info(f"Downloading dataset: {dataset_ref}")
            download_path = self._download_dataset(dataset_ref)
            
            if not download_path:
                logging.error("Failed to download dataset")
                return pa.Table.from_pydict({})
            
            # Load the dataset files
            logging.info("Loading dataset files...")
            arrow_table = self._load_dataset_files(download_path)
            
            # Convert schema to match logical table schema
            if arrow_table.num_rows > 0:
                arrow_table = self._convert_schema(arrow_table)
            
            # Clean up temporary directory
            try:
                shutil.rmtree(download_path)
                logging.info("Cleaned up temporary files")
            except Exception as e:
                logging.warning(f"Failed to clean up temporary directory: {e}")
            
            return arrow_table
            
        except Exception as e:
            logging.error(f"Error in materialize: {e}")
            return pa.Table.from_pydict({})

    def _download_dataset(self, dataset_ref: str) -> str:
        """Download a dataset and return the path to the downloaded files."""
        
        try:
            # Create a temporary directory for the download
            temp_dir = tempfile.mkdtemp()
            logging.info(f"Created temporary directory: {temp_dir}")
            
            # Download the dataset
            self._kaggle_api.dataset_download_files(
                dataset_ref, 
                path=temp_dir, 
                unzip=True
            )
            
            logging.info(f"Successfully downloaded dataset: {dataset_ref}")
            return temp_dir
            
        except Exception as e:
            logging.error(f"Error downloading dataset {dataset_ref}: {e}")
            return None

    def _load_dataset_files(self, download_path: str) -> pa.Table:
        """Load dataset files and convert to Arrow table."""
        
        try:
            # Find all data files in the download directory
            data_files = []
            
            # Look for common data file extensions
            extensions = ['*.csv', '*.json', '*.parquet', '*.xlsx', '*.xls']
            for ext in extensions:
                files = glob.glob(os.path.join(download_path, '**', ext), recursive=True)
                data_files.extend(files)
            
            if not data_files:
                logging.warning("No data files found in downloaded dataset")
                return pa.Table.from_pydict({})
            
            logging.info(f"Found {len(data_files)} data files")
            
            # Load the first data file (you could extend this to handle multiple files)
            data_file = data_files[0]
            logging.info(f"Loading data file: {data_file}")
            
            # Load based on file extension
            if data_file.endswith('.csv'):
                df = pd.read_csv(data_file)
            elif data_file.endswith('.json'):
                df = pd.read_json(data_file)
            elif data_file.endswith('.parquet'):
                df = pd.read_parquet(data_file)
            elif data_file.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(data_file)
            else:
                logging.warning(f"Unsupported file format: {data_file}")
                return pa.Table.from_pydict({})
            
            # Convert to Arrow table
            arrow_table = pa.Table.from_pandas(df)
            
            logging.info(f"Successfully loaded dataset with {arrow_table.num_rows} rows and {arrow_table.num_columns} columns")
            return arrow_table
            
        except Exception as e:
            logging.error(f"Error loading dataset files: {e}")
            return pa.Table.from_pydict({})

    def _convert_schema(self, arrow_table: pa.Table) -> pa.Table:
        """Convert the Arrow table schema to match the logical table schema using DuckDB."""
        
        try:
            # Get the expected schema from logical table
            expected_schema = self._logical_table.get_schema()
            expected_columns = expected_schema.get_attribute_names()
            
            logging.info(f"Expected columns: {expected_columns}")
            logging.info(f"Actual columns: {arrow_table.column_names}")
            
            # Convert to pandas for DuckDB processing
            df = arrow_table.to_pandas()
            
            # Generate SQL query for schema conversion with retry loop
            sql_query = self._generate_schema_conversion_sql_with_retry(df, expected_schema)
            
            logging.info(f"Final SQL query: {sql_query}")
            
            # Store the SQL query for debugging
            self._last_sql_query = sql_query
            
            # Execute query using DuckDB
            conn = duckdb.connect()
            # Register the DataFrame as a temporary table
            conn.register('source_data', df)
            result_df = conn.execute(sql_query).df()
            conn.close()
            
            # Convert back to Arrow table
            converted_table = pa.Table.from_pandas(result_df)
            
            logging.info(f"Schema conversion completed: {converted_table.num_rows} rows, {converted_table.num_columns} columns")
            return converted_table
            
        except Exception as e:
            logging.error(f"Error converting schema: {e}")
            return arrow_table

    def _generate_schema_conversion_sql_with_retry(self, df: pd.DataFrame, expected_schema, max_retries: int = 3) -> str:
        """Generate SQL query with retry loop that includes error feedback."""
        
        last_error = None
        
        for attempt in range(max_retries):
            try:
                logging.info(f"SQL generation attempt {attempt + 1}/{max_retries}")
                
                # Generate SQL query
                sql_query = self._generate_schema_conversion_sql(df, self._logical_table.get_prompt(), expected_schema, last_error, attempt)
                
                # Test the query with DuckDB
                conn = duckdb.connect()
                conn.register('source_data', df)
                
                # Try to execute the query to validate it
                test_result = conn.execute(sql_query).df()
                conn.close()
                
                logging.info(f"SQL query validation successful on attempt {attempt + 1}")
                return sql_query
                
            except Exception as e:
                last_error = str(e)
                logging.warning(f"SQL query failed on attempt {attempt + 1}: {last_error}")
                
                if attempt == max_retries - 1:
                    logging.error(f"All {max_retries} attempts failed, falling back to rule-based generation")
                    return self._generate_fallback_sql(df, expected_schema)
        
        # This should never be reached, but just in case
        return self._generate_fallback_sql(df, expected_schema)

    def _generate_schema_conversion_sql(self, df: pd.DataFrame, prompt, expected_schema, last_error: str = None, attempt: int = 0) -> str:
        """Generate SQL query to convert schema using LLM."""
        
        # Prepare information for LLM
        actual_columns = df.columns.tolist()
        expected_columns = []
        
        for attr in expected_schema.get_attributes():
            expected_columns.append({
                'name': attr.get_name(),
                'type': attr.get_data_type(),
                'description': attr.get_description() or attr.get_name()
            })
        
        # Create LLM prompt with error feedback if this is a retry
        error_context = ""
        if last_error and attempt > 0:
            error_context = f"""

PREVIOUS ATTEMPT FAILED:
The previous SQL query failed with this error: {last_error}

Please fix the query based on this error. Common issues:
- Column names with spaces or special characters need to be quoted with backticks: `column name`
- Invalid data type casting
- Missing columns that don't exist in the source data
- Syntax errors in the SQL

This is attempt {attempt + 1}, please be more careful with the syntax.
"""

        prompt_text = f"""
You are a SQL expert. Generate a DuckDB SQL query that extracts a table with the following description:
{prompt}

The required schema of the table is the following:
{chr(10).join([f"- {col['name']} ({col['type']}): {col['description']}" for col in expected_columns])}

SAMPLE DATA (first 3 rows) from the input table:
{df.head(3).to_string()}
{error_context}

TASK:
Generate a DuckDB SQL query that:
1. Maps actual columns to expected column names
2. Casts data types appropriately
3. Provides default values for missing columns
4. Uses the table name "source_data"

Be careful of the input column names since they are often inconsistent and contain 
special characters (spaces, parentheses, etc). Use backticks around column names if needed.

REQUIREMENTS:
- Return ONLY the SQL query, no explanations
- Use proper DuckDB syntax
- Quote column names with backticks if they contain spaces or special characters
- Be aware that for reserved keywords (like rank), you should wrap them with double-quotes "" if used as columns.
- Don't use backticks in your SQL queries.
"""
        
        try:
            response = self._llm.call([prompt_text])
            
            # Clean up the response to extract just the SQL
            sql_query = response.strip()
            
            # Remove any markdown formatting if present
            if sql_query.startswith('```sql'):
                sql_query = sql_query[6:]
            if sql_query.startswith('```'):
                sql_query = sql_query[3:]
            if sql_query.endswith('```'):
                sql_query = sql_query[:-3]
            
            sql_query = sql_query.strip()
            
            # Validate that it's a SELECT query
            if not sql_query.upper().startswith('SELECT'):
                raise ValueError("Generated query is not a SELECT statement")
            
            logging.info(f"LLM generated SQL query: {sql_query}")
            return sql_query
            
        except Exception as e:
            logging.error(f"Error generating SQL with LLM: {e}")
            # Fallback to simple query
            return self._generate_fallback_sql(df, expected_schema)
    
    def _generate_fallback_sql(self, df: pd.DataFrame, expected_schema) -> str:
        """Generate a simple fallback SQL query if LLM fails."""
        
        select_clauses = []
        
        for attr in expected_schema.get_attributes():
            col_name = attr.get_name()
            col_type = attr.get_data_type()
            
            # Find matching column
            matching_col = self._find_matching_column(col_name, df.columns)
            
            if matching_col:
                # Map the actual column to expected column name
                if col_type == 'str':
                    select_clauses.append(f"CAST({matching_col} AS VARCHAR) AS {col_name}")
                elif col_type in ['int', 'int64']:
                    select_clauses.append(f"CAST({matching_col} AS INTEGER) AS {col_name}")
                elif col_type in ['float', 'float64']:
                    select_clauses.append(f"CAST({matching_col} AS DOUBLE) AS {col_name}")
                elif col_type == 'bool':
                    select_clauses.append(f"CAST({matching_col} AS BOOLEAN) AS {col_name}")
                else:
                    select_clauses.append(f"CAST({matching_col} AS VARCHAR) AS {col_name}")
            else:
                # Create column with default values
                if col_type == 'str':
                    select_clauses.append(f"'N/A' AS {col_name}")
                elif col_type in ['int', 'int64']:
                    select_clauses.append(f"0 AS {col_name}")
                elif col_type in ['float', 'float64']:
                    select_clauses.append(f"0.0 AS {col_name}")
                elif col_type == 'bool':
                    select_clauses.append(f"FALSE AS {col_name}")
                else:
                    select_clauses.append(f"'N/A' AS {col_name}")
                
                logging.warning(f"No matching column found for '{col_name}', using default value")
        
        # Build the complete SQL query
        sql_query = f"""
        SELECT {', '.join(select_clauses)}
        FROM source_data
        """
        
        return sql_query

    def _find_matching_column(self, expected_col: str, actual_columns: List[str]) -> str:
        """Find the best matching column name from actual columns."""
        
        expected_lower = expected_col.lower()
        
        # Exact match
        if expected_col in actual_columns:
            return expected_col
        
        # Case-insensitive match
        for col in actual_columns:
            if col.lower() == expected_lower:
                return col
        
        # Partial match (contains)
        for col in actual_columns:
            if expected_lower in col.lower() or col.lower() in expected_lower:
                return col
        
        # Fuzzy match using common patterns
        fuzzy_patterns = {
            'company_name': ['company', 'name', 'business', 'corp', 'inc'],
            'revenue': ['revenue', 'sales', 'income', 'turnover'],
            'industry': ['industry', 'sector', 'business_type', 'category'],
            'employees': ['employees', 'staff', 'workers', 'headcount'],
            'website': ['website', 'url', 'web', 'site'],
            'profit': ['profit', 'earnings', 'net_income', 'income'],
            'city': ['city', 'location', 'headquarters'],
            'state': ['state', 'province', 'region', 'country']
        }
        
        if expected_lower in fuzzy_patterns:
            patterns = fuzzy_patterns[expected_lower]
            for col in actual_columns:
                col_lower = col.lower()
                for pattern in patterns:
                    if pattern in col_lower:
                        return col
        
        return None

    def _find_dataset(self) -> Dict[str, Any]:
        """Find the best matching Kaggle dataset based on the logical table prompt."""
        
        if not self._kaggle_api:
            logging.error("Kaggle API not available")
            return {}
        
        # Get the table description from logical table
        table_description = self._logical_table.get_prompt()
        
        # Generate search queries using LLM
        search_queries = self._generate_search_queries(table_description)
        
        # Search for datasets using each query
        all_datasets = []
        for query in search_queries:
            try:
                datasets: List[ApiDataset] = self._kaggle_api.dataset_list(
                    search=query,
                )
                
                # Convert to list of dictionaries
                for dataset in datasets:
                    all_datasets.append(dataset.to_dict())
                    
            except Exception as e:
                logging.warning(f"Error searching with query '{query}': {e}")
                continue
        
        # Remove duplicates based on dataset reference
        unique_datasets = {}
        for dataset in all_datasets:
            ref = dataset.get('ref', '')
            if ref and ref not in unique_datasets:
                unique_datasets[ref] = dataset
        
        # Find the best matching dataset using LLM
        if not unique_datasets:
            logging.warning("No datasets found")
            return {}
        
        best_dataset = self._select_best_dataset(list(unique_datasets.values()), table_description)
        
        return best_dataset

    def _generate_search_queries(self, table_description: str) -> List[str]:
        """Generate Kaggle search queries based on table description using LLM."""
        
        prompt = f"""
You are a data expert who helps find relevant datasets on Kaggle. Given a table description, generate 3-5 effective search queries that would help find the most suitable datasets on Kaggle.

Table Description: {table_description}

Generate search queries that:
1. Use relevant keywords from the description
2. Include domain-specific terms
3. Consider different ways to phrase the same concept
4. Are optimized for Kaggle's search functionality

Return only a JSON array of search queries, nothing else.
Example: ["machine learning datasets", "data science competitions", "predictive modeling data"]
"""

        try:
            response = self._llm.call([prompt])
            # Parse the JSON response
            import json
            queries = json.loads(response)
            if isinstance(queries, list):
                return queries
            else:
                # If response is not a list, try to extract queries from text
                return [query.strip() for query in response.split('\n') if query.strip()]
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON response from LLM: {e}")
            # Fallback: try to extract queries from text
            try:
                return [query.strip() for query in response.split('\n') if query.strip()]
            except:
                pass
        except Exception as e:
            logging.warning(f"Failed to generate search queries with LLM: {e}")
        
        # Final fallback: create simple queries from description
        words = table_description.lower().split()
        keywords = [word for word in words if len(word) > 3 and word.isalpha()]
        return keywords[:3]

    def _select_best_dataset(self, datasets: List[Dict[str, Any]], table_description: str) -> Dict[str, Any]:
        """Select the best matching dataset using LLM."""
        
        if not datasets:
            return {}
        
        # Limit to top 10 datasets for LLM evaluation
        top_datasets = datasets[:10]
        
        # Create prompt for dataset selection
        datasets_info = []
        for i, dataset in enumerate(top_datasets):
            # Handle tags properly - they might be strings or dictionaries
            tags = dataset.get('tags', [])
            if tags and isinstance(tags[0], dict):
                # If tags are dictionaries, extract the name field
                tag_names = [tag.get('name', str(tag)) for tag in tags]
            else:
                # If tags are strings, use them directly
                tag_names = [str(tag) for tag in tags]
            
            info = f"""
Dataset {i+1}:
- Title: {dataset.get('title', 'N/A')}
- Description: {str(dataset.get('description', 'N/A'))[:200]}...
- Tags: {', '.join(tag_names)}
- Size: {dataset.get('size', 'N/A')}
- Download Count: {dataset.get('downloadCount', 'N/A')}
- Usability Rating: {dataset.get('usabilityRating', 'N/A')}
- Owner: {dataset.get('owner', 'N/A')}
"""
            datasets_info.append(info)
        
        prompt = f"""
You are a data expert who needs to select the best matching dataset from Kaggle.

Table Description: {table_description}

Available Datasets:
{chr(10).join(datasets_info)}

Please analyze these datasets and select the one that best matches the table description. Consider:
1. Relevance to the description
2. Data quality (usability rating)
3. Popularity (download count)
4. Recency (last updated)
5. Completeness of description

Return only the dataset number (1-{len(top_datasets)}) that best matches the requirements. Return only the number and nothing else.
"""

        try:
            response = self._llm.call([prompt])
            # Extract number from response
            import re
            numbers = re.findall(r'\d+', response)
            if numbers:
                selected_index = int(numbers[0]) - 1
                if 0 <= selected_index < len(top_datasets):
                    return top_datasets[selected_index]
            
            # Fallback: return the first dataset
            return top_datasets[0]
            
        except Exception as e:
            logging.warning(f"Failed to select best dataset with LLM: {e}")
            # Fallback: return the first dataset
            return top_datasets[0]