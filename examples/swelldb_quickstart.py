from swelldb import SwellDB

swelldb: SwellDB = SwellDB()

table_builder = swelldb.table_builder()
table_builder.set_content("A table that contains all the US states")
table_builder.set_schema("state_name str, region str")

tbl = table_builder.build()

# Explore the table generation plan
tbl.explain()

# Create the table
table = tbl.materialize()

print(table.to_pandas())