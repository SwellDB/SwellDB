# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os

import pandas as pd
from pandas import DataFrame

from swelldb.datasource.datasource import Datasource


class LocalCSVDatasource(Datasource):
    def __init__(self, directory: str):
        self.directory = directory
        self.csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    def search(self, query: str) -> list[DataFrame]:
        results: dict[str, DataFrame] = dict()
        for csv_file in self.csv_files:
            results[csv_file.split(".")[0]] = pd.read_csv(
                os.path.join(self.directory, csv_file)
            )

        return results
