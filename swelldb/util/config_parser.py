# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import configparser
import os

import yaml


class ConfigParser:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = {}

    @staticmethod
    def get_config(config_name: str) -> str:
        # Get current dir
        current_dir = os.path.dirname(__file__)
        # Get the path of the config file
        config_file = os.path.join(current_dir, "../../config", "config.yaml")

        with open(config_file, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
            return data["global"][config_name]
