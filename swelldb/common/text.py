from typing import List

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document

class Splitter:
    def __init__(self, chunk_size: int = 4000, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def split(self, text: str) -> List[str]:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        return splitter.split_text(text)