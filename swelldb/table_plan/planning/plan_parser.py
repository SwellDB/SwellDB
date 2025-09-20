from swelldb import SwellDBSchema, SwellDB
from swelldb.table_plan.meta import TableConfig
from swelldb.table_plan.mode import Mode
from swelldb.table_plan.table.logical.logical_table import LogicalTable
from swelldb.table_plan.table.physical import ImageTable


def parse_plan(plan_json: str, swelldb: SwellDB):
    prompt = plan_json["prompt"]
    meta = TableConfig()

    # schema = SwellDBSchema.from_string("listing_path str, has_natural_light bool, price double")

    for operator in plan_json["operators"]:
        if operator == "ImageTable":
            for image in plan_json["operators"][operator]["files"]: meta.add_image(image)
        elif operator == "RawTextTable":
            for txt in plan_json["operators"][operator]["files"]: meta.add_text_file(txt)
        elif operator == "DocumentTable":
            for doc in plan_json["operators"][operator]["files"]: meta.add_link(doc)

    # Create tables based on the plan using the table builder
    tables = []

    # Process each operator in the plan
    for operator in plan_json["operators"]:
        schema_str = ", ".join([f"{attr}" for attr in plan_json["operators"][operator]["attributes"]])

        if not schema_str:
            continue

        schema = SwellDBSchema.from_string(schema_str)

        if operator == "ImageTable":
            # Create ImageTable with attributes from plan
            print(f"Creating ImageTable with schema: {schema_str}")
            image_logical_table = LogicalTable("", prompt, schema)
            image_table = ImageTable(
                logical_table=image_logical_table,
                child_table=None,
                meta=meta,
                llm=swelldb._llm
            )
            tables.append(image_table)

        elif operator == "RawTextTable":
            # Create RawTextTable with attributes from plan
            print(f"Creating RawTextTable with schema: {schema_str}")
            rawtext_logical_table = LogicalTable("", prompt, schema)
            from swelldb.table_plan.table.physical.rawtext_table import RawTextTable
            rawtext_table = RawTextTable(
                logical_table=rawtext_logical_table,
                child_table=None,
                meta=meta,
                llm=swelldb._llm
            )
            tables.append(rawtext_table)
        elif operator == "DocumentTable":
            # Create DocumentTable with attributes from plan
            print(f"Creating DocumentTable with schema: {schema_str}")
            doc_logical_table = LogicalTable("", prompt, schema)
            from swelldb.table_plan.table.physical.document_table import DocumentTable
            doc_table = DocumentTable(
                logical_table=doc_logical_table,
                child_table=None,
                meta=meta,
                llm=swelldb._llm
            )
            tables.append(doc_table)

    # Execute the plan
    print("Executing plan...")
    results = []

    for table in tables:
        print(f"Processing {table.__class__.__name__}...")
        result = table.materialize()
        results.append(result)

    return results