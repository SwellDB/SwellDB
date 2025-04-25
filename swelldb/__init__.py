# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

"""
SwellDB - A novel database system leveraging Large Language Models for natural language querying.
"""

from swelldb.swelldb import SwellDB
from swelldb.table_plan.planner import TableGenPlanner
from swelldb.llm.openai_llm import OpenAILLM
from swelldb.table_plan.swelldb_schema import SwellDBSchemaBuilder, SwellDBSchema
from swelldb.catalog import Catalog

__version__ = "0.1.0"
__all__ = [
    "SwellDB",
    "OpenAILLM",
    "SwellDBSchemaBuilder",
    "SwellDBSchema",
    "Catalog",
]
