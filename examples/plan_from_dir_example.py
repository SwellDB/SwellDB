from swelldb.table_plan.planning.agentic.plan_from_dir_agent import PlanFromDirectoryAgent

if __name__ == "__main__":
    agent = PlanFromDirectoryAgent()
    result = agent.run_task("""
    "Create a table that contains information about these listings. Pick only one picture per listing.

    The table should have the following schema:

    listing_path: str, has_natural_light: bool, price: str, is_attractive: str, distance_from_mit: double
    """,
                            "path")


    r = result["result"]