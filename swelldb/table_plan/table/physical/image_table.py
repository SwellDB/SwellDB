# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
import base64
import glob
from typing import List, Dict
import logging

from overrides import override
import pyarrow as pa

from swelldb.llm.abstract_llm import AbstractLLM
from swelldb.table_plan.meta import SwellDBMeta
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical.physical_table import PhysicalTable
from swelldb.engine.execution_engine import ExecutionEngine
from swelldb.prompt.prompt_utils import create_table_prompt


class ImageTable(PhysicalTable):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        logical_table: LogicalTable,
        child_table: PhysicalTable,
        meta: SwellDBMeta,
        llm: AbstractLLM,
    ):
        super().__init__(
            logical_table=logical_table,
            child_table=child_table,
            layout=meta.get_layout(),
            operator_name="image_table",
            llm=llm,
            base_columns=meta.get_base_columns(),
            execution_engine=execution_engine,
            chunk_size=meta.get_chunk_size(),
        )

        self._execution_engine = execution_engine
        self._meta = meta

    def get_prompts(self, input_table: pa.Table) -> List[str]:
        """Generate prompts from images in the input folder."""
        logging.info("Processing images for ImageTable")
        
        # Get image paths from meta images
        image_paths = self._meta.get_images() if self._meta.get_images() else []
        
        if not image_paths:
            logging.warning("No image paths provided in meta.images")
            return []
        
        prompts = []
        for image_path in image_paths:
            try:
                # Check if the path is a directory and expand it
                if os.path.isdir(image_path):
                    # Get all image files from the directory
                    image_extensions = ['*.jpg', '*.jpeg', '*.png', '*.gif', '*.bmp', '*.tiff', '*.webp']
                    image_files = []
                    for ext in image_extensions:
                        image_files.extend(glob.glob(os.path.join(image_path, ext)))
                        image_files.extend(glob.glob(os.path.join(image_path, ext.upper())))
                    
                    # Process each image file
                    for img_file in image_files:
                        prompt = self._create_image_prompt(img_file)
                        if prompt:
                            prompts.append(prompt)
                else:
                    # Single image file
                    prompt = self._create_image_prompt(image_path)
                    if prompt:
                        prompts.append(prompt)
                        
            except Exception as e:
                logging.error(f"Failed to process image path {image_path}: {e}")
                continue
        
        if not prompts:
            logging.warning("No image prompts generated")
        
        return prompts

    def _create_image_prompt(self, image_path: str) -> str:
        """Create a prompt for a single image."""
        try:
            # Check if file exists and is readable
            if not os.path.isfile(image_path):
                logging.warning(f"Image file not found: {image_path}")
                return None
            
            # Read and encode the image
            with open(image_path, "rb") as f:
                image_data = f.read()
                image_base64 = base64.b64encode(image_data).decode("utf-8")
            
            # Determine image format from file extension
            image_format = os.path.splitext(image_path)[1].lower().lstrip('.')
            if image_format == 'jpg':
                image_format = 'jpeg'
            
            # Create the image data URL
            image_url = f"data:image/{image_format};base64,{image_base64}"
            
            # Create prompt using the existing prompt utility
            prompt = create_table_prompt(
                table_description=self._logical_table.get_prompt(),
                table_schema=self._logical_table.get_schema().get_attribute_names(),
                data=f"Image file: {os.path.basename(image_path)}\nImage path: {image_path}\nImage data: {image_url}",
                layout=self._layout,
            )
            
            return prompt
            
        except Exception as e:
            logging.error(f"Failed to create prompt for image {image_path}: {e}")
            return None

    @override
    def __str__(self):
        return f'ImageTable[schema={self._logical_table.get_schema().get_attribute_names()}]'
