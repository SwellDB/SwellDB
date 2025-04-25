# Copyright (c) 2025 Victor Giannakouris
# 
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import unittest
import pyarrow as pa

from swelldb.table_plan.swelldb_schema import SwellDBSchemaBuilder, SwellDBSchema
from swelldb.table_plan.table.logical.logical_table import LogicalTable


class TestSwellDBUsage(unittest.TestCase):
    def test_schema_1(self):
        builder: SwellDBSchemaBuilder = SwellDBSchemaBuilder()

        schema: SwellDBSchema = (
            builder.add_attribute("name", pa.string(), description="just a name")
            .add_attribute("age", pa.int32(), description=None)
            .add_attribute("text_field", pa.string(), description=None)
        ).build()

        self.assertEqual(schema.get_attribute("name").get_name(), "name")
        self.assertEqual(schema.get_attribute("age").get_name(), "age")
        self.assertEqual(schema.get_attribute("text_field").get_name(), "text_field")