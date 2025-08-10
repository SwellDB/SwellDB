# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import json

from langchain_core.language_models import BaseChatModel


def _contains_image_data(prompt: str) -> bool:
    """Check if the prompt contains base64 image data."""
    return "data:image/" in prompt and "base64," in prompt


class AbstractLLM:
    def __init__(self, llm: BaseChatModel):
        self.llm: BaseChatModel = llm

        # Stats
        self.input_tokens = 0
        self.output_tokens = 0

    def call(self, prompt: str) -> str:
        # Check if this is a multimodal prompt with image data
        if _contains_image_data(prompt):
            return self._call_multimodal(prompt)
        
        # Regular text-only prompt
        r = self.llm.invoke(prompt)
        stats = r.usage_metadata

        self.input_tokens += stats["input_tokens"]
        self.output_tokens += stats["output_tokens"]

        r = r.content

        if "```json" in r:
            r = r.split("```json")[1].split("```")[0]
        else:
            r = r

        # For Deepseek responses
        if "</think>" in r:
            r = r.split("</think>")[1]

        return r

    def _call_multimodal(self, prompt: str) -> str:
        """Handle multimodal prompts with images. Override in subclasses."""
        raise NotImplementedError("Multimodal prompts not supported by this LLM implementation")