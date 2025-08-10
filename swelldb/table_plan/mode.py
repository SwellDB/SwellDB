# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

from enum import Enum


class Mode(Enum):
    PLANNER = "planner"
    OPERATORS = "operators"
    LLM = "llm"
    SEARCH = "search"
    DATASET = "dataset"
    DOCUMENT = "document"
    IMAGE = "image" 