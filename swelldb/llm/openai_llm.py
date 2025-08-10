# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.
import logging
import os

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.util.globals import Globals
from swelldb.util.config import Config


class OpenAILLM(AbstractLLM):
    def __init__(self, model: str, api_key: str = None, temperature: int = 0) -> None:
        self.api_key: str = ""

        if api_key:
            self.api_key = api_key
        elif os.getenv(Globals.OPENAI_API_KEY):
            self.api_key = os.getenv(Globals.OPENAI_API_KEY)
        else:
            # Try config file as fallback
            config = Config()
            self.api_key = config.get_openai_api_key() or ""

        llm = ChatOpenAI(
            api_key=self.api_key,
            temperature=temperature,
            model=model,
        )
        super().__init__(llm=llm)

    def _call_multimodal(self, prompt: str) -> str:
        """Handle multimodal prompts with images for OpenAI vision models."""
        try:
            # Extract the text content and image data from the prompt
            text_content, image_data = self._parse_multimodal_prompt(prompt)
            
            logging.debug(f"Extracted text content length: {len(text_content) if text_content else 0}")
            logging.debug(f"Extracted image data length: {len(image_data) if image_data else 0}")
            if image_data:
                logging.debug(f"Image data starts with: {image_data[:100]}...")
            
            # Create the message content
            content = []
            
            # Add text content
            if text_content.strip():
                content.append({"type": "text", "text": text_content})
            
            # Add image content
            if image_data:
                content.append({
                    "type": "image_url", 
                    "image_url": {"url": image_data}
                })
            
            # Create the message
            message = HumanMessage(content=content)
            
            # Call the LLM
            response = self.llm.invoke([message])
            
            # Extract stats if available
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                stats = response.usage_metadata
                self.input_tokens += stats.get("input_tokens", 0)
                self.output_tokens += stats.get("output_tokens", 0)
            
            result = response.content
            
            # Clean up the response (same logic as text-only)
            if "```json" in result:
                result = result.split("```json")[1].split("```")[0]
            
            return result
            
        except Exception as e:
            # Fallback to text-only if multimodal fails
            logging.warning(f"Multimodal processing failed, falling back to text-only: {e}")
            return super().call(prompt)

    def _parse_multimodal_prompt(self, prompt: str) -> tuple[str, str]:
        """Parse a multimodal prompt to extract text and image data."""
        logging.debug(f"Parsing prompt of length: {len(prompt)}")
        
        # Find the start of the image data URL
        start_marker = "Image data: "
        start_idx = prompt.find(start_marker)
        
        logging.debug(f"Found start marker at index: {start_idx}")
        
        if start_idx == -1:
            logging.debug("No start marker found")
            return prompt, None
        
        # Extract everything after "Image data: "
        image_data_with_prefix = prompt[start_idx + len(start_marker):]
        
        # Find the end of the image data (look for the next newline or end of string)
        end_idx = image_data_with_prefix.find('\n')
        if end_idx == -1:
            end_idx = len(image_data_with_prefix)
        
        logging.debug(f"Found end marker at index: {end_idx}")
        
        image_data = image_data_with_prefix[:end_idx].strip()
        
        logging.debug(f"Extracted image data length: {len(image_data)}")
        logging.debug(f"Image data starts with: {image_data[:100] if image_data else 'None'}...")
        
        # Verify it's a valid data URL
        if not image_data.startswith('data:image/') or ';base64,' not in image_data:
            logging.debug("Invalid image data format")
            return prompt, None
        
        # Remove the entire image data section from the text
        text_content = prompt[:start_idx] + prompt[start_idx + len(start_marker) + end_idx:].strip()
        
        logging.debug(f"Final text content length: {len(text_content)}")
        
        return text_content, image_data
