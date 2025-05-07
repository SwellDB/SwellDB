[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python application](https://github.com/SwellDB/SwellDB/actions/workflows/python-app.yml/badge.svg)](https://github.com/SwellDB/SwellDB/actions/workflows/python-app.yml)
<a href="https://pypi.org/project/swelldb" target="_blank">
    <img src="https://img.shields.io/pypi/v/swelldb?color=%2334D058&label=pypi%20package" alt="Package version">
</a>

 # SwellDB

**Query any data — from LLMs, databases, or the web — using just DataFrames or SQL**

## Overview

**SwellDB** is a new kind of data system that enables SQL-based analytical querying over **dynamically generated tables**. These tables are synthesized in real-time from a combination of sources, including:

- Large Language Models (LLMs)
- Existing databases
- File formats (e.g., CSV, Parquet)
- Web search results

Unlike traditional systems operating under a closed-world assumption (queries only run on pre-loaded data), **SwellDB generates tables on-demand**, tailored to user-defined prompts and schemas.

This enables bridging structured SQL querying with the flexibility of unstructured data retrieval.

<div align="center">
  <img src="https://raw.githubusercontent.com/SwellDB/SwellDB/main/images/swelldb_architecture.png" alt="SwellDB Architecture" width="300"/>
  <p><em>Figure: SwellDB Architecture</em></p>
</div>

## Key Features

- **🔄 Dynamic Table Generation**  
  Automatically synthesizes tables on-the-fly from queries and schema prompts — no need for preloaded data.

- **🌐 Multi-Source Integration**  
  Combines data from:
  - Large Language Models (LLMs)
  - Structured sources (e.g., CSV, SQL databases)
  - Unstructured sources (e.g., web pages, text files)
  - Web search results

- **🧠 LLM-Powered Reasoning**  
  Uses LLMs to:
  - Generate SQL queries over datasets  
  - Extract, augment, and synthesize missing information  
  - Transform unstructured text into structured tables

- **🧩 Modular & Extensible**  
  Easy to plug in new data sources via a clean Data Source API (structured + unstructured).

- **🧪 Fully SQL-Compatible**  
  Query generated tables with standard SQL — powered by [Apache DataFusion](https://datafusion.apache.org/).

- **🌍 Open-World Query Execution**  
  Go beyond what’s stored — SwellDB fetches or generates the missing pieces on demand.

- **⚡ Seamless Developer Experience**  
  Define tables declaratively using natural language and schema annotations. Then just write SQL.

## Use Cases — Examples

- **Populating relational databases from unstructured sources**  
  Generate tables for a relational database with DSA interview questions in [SQLite](examples/swell_dsa_questions.ipynb).

- **Ad-hoc querying across hybrid sources**  
  Seamlessly blend local CSVs, remote databases, LLM completions, and web results into a unified DataFrame. See [example](examples/swelldb_mutations.ipynb).

- **Building completely new tables on-the-fly**  
  Dynamically generate subject-specific datasets without predefining complex ETL pipelines. See [example](examples/swelldb_basic.ipynb).

## 🚀 Get Started

### Install SwellDB

```bash
pip install swelldb
```

### Obtain OpenAI API Key
To run the following example, you need to obtain an API key for OpenAI. 
You can sign up for OpenAI [here](https://platform.openai.com/signup). Then
you can set the API keys as environment variables:

```bash
export OPENAI_API_KEY=your_openai_api_key
```

### Create a table

```python
from swelldb import SwellDB

swelldb: SwellDB = SwellDB()

table_builder = swelldb.table_builder()
table_builder.set_content("A table that contains all the US states")
table_builder.set_schema("state_name str, region str")

tbl = table_builder.build()

# Explore the table generation plan
tbl.explain()

# Create the table
table = tbl.materialize()

print(table.to_pandas())
```
#### Output
```
    state_name     region
0      Alabama      South
1       Alaska       West
2      Arizona       West
3     Arkansas      South
4   California       West
```

### Querying with SQL using DataFusion
```python
import datafusion
import pyarrow as pa

sc = datafusion.SessionContext()
sc.register_dataset("us_states", pa.dataset.dataset(table))

# Get 5 states from the South region
print(sc.sql("SELECT * FROM us_states where region = 'South' LIMIT 5"))

# Count the number of states per region
print(sc.sql("SELECT COUNT(*), region FROM us_states GROUP BY region"))
```

#### Output
```
DataFrame()
+------------+--------+
| state_name | region |
+------------+--------+
| Alabama    | South  |
| Arkansas   | South  |
| Delaware   | South  |
| Florida    | South  |
| Georgia    | South  |
+------------+--------+
DataFrame()
+----------+-----------+
| count(*) | region    |
+----------+-----------+
| 12       | Midwest   |
| 9        | Northeast |
| 16       | South     |
| 13       | West      |
+----------+-----------+
```
