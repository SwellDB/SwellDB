# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from .custom_table import CustomTable
from .dataset_table import DatasetTable
from .document_table import DocumentTable
from .image_table import ImageTable
from .llm_table import LLMTable
from .physical_table import PhysicalTable
from .rawtext_table import RawTextTable
from .search_engine_table import SearchEngineTable

__all__ = [
    "CustomTable",
    "DatasetTable", 
    "DocumentTable",
    "ImageTable",
    "LLMTable",
    "PhysicalTable",
    "RawTextTable",
    "SearchEngineTable",
]
