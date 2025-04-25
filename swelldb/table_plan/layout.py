# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

class Layout:
    def get_name(self) -> str:
        return self.__class__.__name__.upper()

    _ROW = None
    _COLUMN = None

    @classmethod
    def ROW(cls) -> "Layout":
        if cls._ROW is None:
            cls._ROW = Row()
        return cls._ROW

    @classmethod
    def COLUMN(cls) -> "Layout":
        if cls._COLUMN is None:
            cls._COLUMN = Column()
        return cls._COLUMN


class Column(Layout):
    pass


class Row(Layout):
    pass
