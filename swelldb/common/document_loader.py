# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
from typing import List, Dict, Optional
from pathlib import Path
import logging

try:
    import PyPDF2
    import pdfplumber
    from docx import Document as DocxDocument
except ImportError as e:
    logging.warning(f"Document processing dependencies not installed: {e}")
    PyPDF2 = None
    pdfplumber = None
    DocxDocument = None


class DocumentLoader:
    """Loads and extracts text from various document formats."""
    
    def __init__(self, chunk_size: int = 4000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
    def load_pdf(self, file_path: str, method: str = "pdfplumber") -> str:
        """
        Load and extract text from a PDF file.
        
        Args:
            file_path: Path to the PDF file
            method: 'pypdf2' or 'pdfplumber' (default: pdfplumber)
            
        Returns:
            Extracted text content
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"PDF file not found: {file_path}")
            
        if method == "pypdf2" and PyPDF2:
            return self._load_pdf_pypdf2(file_path)
        elif method == "pdfplumber" and pdfplumber:
            return self._load_pdf_pdfplumber(file_path)
        else:
            raise ImportError(f"PDF processing library not available for method: {method}")
    
    def _load_pdf_pypdf2(self, file_path: str) -> str:
        """Load PDF using PyPDF2."""
        text = ""
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text += page.extract_text() + "\n"
        return text
    
    def _load_pdf_pdfplumber(self, file_path: str) -> str:
        """Load PDF using pdfplumber (better for tables and complex layouts)."""
        text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text
    
    def load_docx(self, file_path: str) -> str:
        """Load and extract text from a DOCX file."""
        if not DocxDocument:
            raise ImportError("python-docx not installed")
            
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"DOCX file not found: {file_path}")
            
        doc = DocxDocument(file_path)
        text = ""
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"
        return text
    
    def load_txt(self, file_path: str, encoding: str = "utf-8") -> str:
        """Load text from a plain text file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Text file not found: {file_path}")
            
        with open(file_path, 'r', encoding=encoding) as file:
            return file.read()
    
    def load_document(self, file_path: str, **kwargs) -> str:
        """
        Auto-detect document type and load accordingly.
        
        Args:
            file_path: Path to the document
            **kwargs: Additional arguments for specific loaders
            
        Returns:
            Extracted text content
        """
        file_path = Path(file_path)
        extension = file_path.suffix.lower()
        
        if extension == '.pdf':
            return self.load_pdf(str(file_path), **kwargs)
        elif extension == '.docx':
            return self.load_docx(str(file_path))
        elif extension in ['.txt', '.md']:
            return self.load_txt(str(file_path), **kwargs)
        else:
            raise ValueError(f"Unsupported document type: {extension}")
    
    def extract_tables_from_pdf(self, file_path: str) -> List[Dict]:
        """
        Extract tables from PDF using pdfplumber.
        
        Returns:
            List of dictionaries representing tables
        """
        if not pdfplumber:
            raise ImportError("pdfplumber not installed")
            
        tables = []
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_tables = page.extract_tables()
                for table_num, table in enumerate(page_tables):
                    if table:
                        tables.append({
                            'page': page_num + 1,
                            'table_index': table_num,
                            'data': table
                        })
        return tables
