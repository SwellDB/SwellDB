# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.
import json
import os
from typing import Dict, Any

try:
    from langchain.agents import AgentExecutor, create_tool_calling_agent
    from langchain.prompts import ChatPromptTemplate
    from langchain_core.messages import HumanMessage, AIMessage
    from langchain_openai import ChatOpenAI
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    print("Warning: LangChain not available. Install with: pip install langchain langchain-openai")

from swelldb.table_plan.planning.agentic.agent_tools import AgentTools


class PlanFromDirectoryAgent:
    """
    Interactive directory navigation agent using LangChain.
    """
    
    def __init__(self, llm=None, model: str = "gpt-4o"):
        """
        Initialize the LangChain agent.
        
        Args:
            llm: Optional LangChain LLM instance. If None, creates ChatOpenAI using SwellDB config.
            model: OpenAI model to use (default: gpt-4o)
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("LangChain is required but not installed. Install with: pip install langchain langchain-openai")
        
        self.current_path = os.getcwd()
        self.history = []
        self.task = ""
        
        # Initialize tools
        self.agent_tools = AgentTools(self.current_path)
        
        # Initialize LLM using SwellDB's config for API key
        if llm is None:
            api_key = self._get_openai_api_key()
            self.llm = ChatOpenAI(model=model, api_key=api_key, model_kwargs={"response_format": {"type": "json_object"}})
        else:
            self.llm = llm
        
        # Create tools
        self.tools = self.agent_tools.create_langchain_tools()
        
        # Create agent
        self.agent_executor = self._create_agent()
    
    def _get_openai_api_key(self) -> str:
        """
        Get OpenAI API key using SwellDB's configuration system.
        
        Returns:
            OpenAI API key
            
        Raises:
            ValueError: If API key cannot be found
        """
        try:
            # Try SwellDB's config system first
            from swelldb.util.config import Config
            config = Config()
            api_key = config.get_openai_api_key()
            if api_key:
                return api_key
        except Exception:
            pass
        
        # Fallback to environment variable
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            return api_key
        
        # Final fallback - prompt user
        api_key = input("Enter OpenAI API key: ").strip()
        if not api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable or configure in SwellDB config.")
        
        return api_key
    
    def _create_agent(self) -> AgentExecutor:
        """Create the LangChain agent executor."""

        # Create prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an intelligent data pipeline expert. Your task is to create structured tables given
            a set of input data sources. You will need to create a table generation plan using SwellDB's table generation operators. These are the following:
            
            ImageTable: Creates a table from a set of images.
            RawTextTable: Creates a table from a set of input txt files.
            
            Once the tables are created, they should be joined on a common key to construct the final table. Your task 
            is to assign a list of files to each operator. Return the result as a JSON dict of the following keys:
            
            "ImageTable": {{"files": the list of files, "attributes": [the list of attributes and their data types to create from these files (eg attr1 str)] }}
            "RawTextTable": {{"files": the list of files, "attributes": [the list of attributes to create from these files (eg attr1 str)] }}
            "DocumentTable": {{"files": the list of files, "attributes": [the list of attributes to create from these files (eg attr1 str)] }}
            "join_key": a join key to join the individual tables.
            
            Return only a JSON response and nothing else.
           

Available tools:
- ls: List directory contents
- get_current_path: Get current directory path
- set_current_path: Change current directory
- get_path_info: Get detailed info about a path
- explore_directory: Analyze directory contents

Guidelines:
1. Always focus current path and its subdirectories. Never go back.
2. Use ls or explore_directory to understand directory contents
3. Make intelligent decisions about which directories to explore based on the user's task
4. When you find relevant content, provide a summary
5. If the task is complete, say "TASK_COMPLETE"
6. Be systematic and thorough in your exploration
7. Explain your reasoning for each decision

Current task: {task}

Your output should be only the JSON response and no additional text.
"""),
            ("user", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ])

        # Create agent (using bound LLM with tools)
        agent = create_tool_calling_agent(self.llm, self.tools, prompt)

        # Create agent executor
        agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            max_iterations=20,
            early_stopping_method="generate"
        )

        return agent_executor

    def run_task(self, task: str, start_path: str = None) -> Dict[str, Any]:
        """
        Run a directory navigation task using LangChain agent.

        Args:
            task: Natural language task description
            start_path: Starting directory path

        Returns:
            Task execution results
        """
        self.task = task
        self.current_path = start_path or os.getcwd()
        self.agent_tools.current_path = self.current_path
        self.history = []

        print(f"Task: {task}")
        print(f"Starting in: {self.current_path}")
        print("=" * 60)

        try:
            # Run the agent
            result = self.agent_executor.invoke({
                "input": f"Task: {task}. Start exploring from: {self.current_path}",
                "task": task
            })

            print(f"\nAgent Response:")
            print(result["output"])

            return {
                "success": True,
                "task": task,
                "result": result["output"],
                "current_path": self.current_path,
                "history": self.history
            }

        except Exception as e:
            print(f"Error running task: {e}")
            return {
                "success": False,
                "task": task,
                "error": str(e),
                "current_path": self.current_path,
                "history": self.history
            }

    def get_exploration_summary(self) -> Dict[str, Any]:
        """Get a summary of the exploration."""
        return {
            "task": self.task,
            "current_path": self.current_path,
            "history_length": len(self.history),
            "tools_available": [tool.name for tool in self.tools]
        }


if __name__ == "__main__":
    agent = PlanFromDirectoryAgent()
    result = agent.run_task("""
    "Create a table that contains information about these listings. Pick only one picture per listing.

    The table should have the following schema:

    listing_path: str, has_natural_light: bool, price: str, is_attractive: str, distance_from_mit: double
    """,
                            "path")


    r = result["result"]