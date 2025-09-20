# Copyright (c) 2025 Victor Giannakouris
#
# This file is part of SwellDB and is licensed under the MIT License.
# See the LICENSE file in the project root for more information.

import os
import subprocess
from typing import List, Dict, Any

try:
    from langchain.tools import Tool, StructuredTool
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class AgentTools:
    """Collection of tools for directory navigation and file operations."""
    
    def __init__(self, current_path: str = None):
        """
        Initialize the agent tools.
        
        Args:
            current_path: Current working directory path
        """
        self.current_path = current_path or os.getcwd()
    
    def ls_tool(self, path: str) -> str:
        """
        List directory contents.

        Args:
            path: Directory path to list

        Returns:
            Formatted directory listing
        """
        try:
            if not os.path.exists(path):
                return f"Error: Path does not exist: {path}"

            result = subprocess.run(
                ["ls", "-la", path],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                contents = self._parse_ls_output(result.stdout)
                return self._format_ls_output(path, contents)
            else:
                return f"Error: {result.stderr}"

        except subprocess.TimeoutExpired:
            return "Error: Command timed out"
        except Exception as e:
            return f"Error: {str(e)}"
    
    def cat_tool(self, file_path: str) -> str:
        """
        Read and display file contents.
        
        Args:
            file_path: Path to the file to read
            
        Returns:
            File contents or error message
        """
        try:
            if not os.path.exists(file_path):
                return f"Error: File does not exist: {file_path}"
            
            if not os.path.isfile(file_path):
                return f"Error: Path is not a file: {file_path}"
            
            # Check file size to avoid reading huge files
            file_size = os.path.getsize(file_path)
            if file_size > 1024 * 1024:  # 1MB limit
                return f"Error: File too large ({file_size} bytes). Use a smaller file or specific line ranges."
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Limit output length for very long files
            if len(content) > 10000:  # 10KB limit
                content = content[:10000] + "\n... (truncated, file too long)"
            
            return f"Contents of {file_path}:\n\n{content}"
            
        except PermissionError:
            return f"Error: Permission denied reading file: {file_path}"
        except UnicodeDecodeError:
            return f"Error: Cannot decode file as text: {file_path}"
        except Exception as e:
            return f"Error reading file: {str(e)}"
    
    def get_current_path(self) -> str:
        """Get the current working directory path."""
        return self.current_path
    
    def set_current_path(self, path: str) -> str:
        """
        Set the current working directory path.
        
        Args:
            path: New directory path
            
        Returns:
            Confirmation message
        """
        if os.path.exists(path):
            self.current_path = path
            return f"Current path set to: {path}"
        else:
            return f"Error: Path does not exist: {path}"
    
    def get_path_info(self, path: str) -> str:
        """
        Get detailed information about a path.
        
        Args:
            path: Path to analyze
            
        Returns:
            Path information
        """
        try:
            if not os.path.exists(path):
                return f"Path does not exist: {path}"
            
            stat = os.stat(path)
            is_dir = os.path.isdir(path)
            is_file = os.path.isfile(path)
            
            info = f"Path: {path}\n"
            info += f"Type: {'Directory' if is_dir else 'File'}\n"
            info += f"Size: {stat.st_size} bytes\n"
            info += f"Modified: {stat.st_mtime}\n"
            
            if is_dir:
                try:
                    contents = os.listdir(path)
                    info += f"Contents: {len(contents)} items\n"
                    if contents:
                        info += f"Items: {', '.join(contents[:10])}"
                        if len(contents) > 10:
                            info += f" (and {len(contents) - 10} more)"
                except PermissionError:
                    info += "Contents: Permission denied"
            
            return info
            
        except Exception as e:
            return f"Error getting path info: {str(e)}"
    
    def explore_directory(self, path: str) -> str:
        """
        Explore a directory and provide analysis.
        
        Args:
            path: Directory path to explore
            
        Returns:
            Directory analysis
        """
        try:
            if not os.path.exists(path):
                return f"Error: Path does not exist: {path}"
            
            if not os.path.isdir(path):
                return f"Error: Path is not a directory: {path}"
            
            contents = os.listdir(path)
            directories = []
            files = []
            
            for item in contents:
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    directories.append(item)
                else:
                    files.append(item)
            
            analysis = f"Directory Analysis for: {path}\n"
            analysis += f"Total items: {len(contents)}\n"
            analysis += f"Directories: {len(directories)}\n"
            analysis += f"Files: {len(files)}\n\n"
            
            if directories:
                analysis += "Directories:\n"
                for d in directories[:10]:
                    analysis += f"  📁 {d}/\n"
                if len(directories) > 10:
                    analysis += f"  ... and {len(directories) - 10} more directories\n"
            
            if files:
                analysis += "\nFiles:\n"
                for f in files[:10]:
                    analysis += f"  📄 {f}\n"
                if len(files) > 10:
                    analysis += f"  ... and {len(files) - 10} more files\n"
            
            return analysis
            
        except Exception as e:
            return f"Error exploring directory: {str(e)}"
    
    def _parse_ls_output(self, ls_output: str) -> List[Dict[str, Any]]:
        """Parse ls output into structured data."""
        contents = []

        for line in ls_output.split('\n'):
            line = line.strip()
            if line and not line.startswith('total'):
                parts = line.split()
                if len(parts) >= 9:
                    permissions = parts[0]
                    links = parts[1]
                    owner = parts[2]
                    group = parts[3]
                    size = parts[4]
                    date_parts = parts[5:8]
                    name = ' '.join(parts[8:])

                    is_directory = permissions.startswith('d')
                    full_path = os.path.join(self.current_path, name)

                    contents.append({
                        "name": name,
                        "permissions": permissions,
                        "links": links,
                        "owner": owner,
                        "group": group,
                        "size": size,
                        "date": ' '.join(date_parts),
                        "is_directory": is_directory,
                        "is_file": not is_directory,
                        "full_path": full_path
                    })

        return contents

    def _format_ls_output(self, path: str, contents: List[Dict[str, Any]]) -> str:
        """Format ls output for display."""
        if not contents:
            return f"Directory {path} is empty"

        output = f"Contents of {path}:\n"

        # Separate directories and files
        directories = [item for item in contents if item["is_directory"] and item["name"] not in [".", ".."]]
        files = [item for item in contents if item["is_file"]]

        if directories:
            output += "\nDirectories:\n"
            for item in directories:
                output += f"  📁 {item['name']}/\n"

        if files:
            output += "\nFiles:\n"
            for item in files:
                output += f"  📄 {item['name']}\n"

        return output
    
    def create_langchain_tools(self) -> List[Tool]:
        """
        Create LangChain tools for directory operations.
        
        Returns:
            List of LangChain Tool objects
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("LangChain is required but not installed. Install with: pip install langchain langchain-openai")
        
        # Create LangChain tools using AgentTools methods
        tools = [
            Tool(
                name="ls",
                func=self.ls_tool,
                description="List directory contents. Use this to see what's in a directory. Input should be the directory path."
            ),
            StructuredTool.from_function(
                func=self.get_current_path,
                name="get_current_path",
                description="Get the current working directory path. No input required."
            ),
            Tool(
                name="set_current_path",
                func=self.set_current_path,
                description="Set the current working directory path. Input should be the new directory path."
            ),
            Tool(
                name="get_path_info",
                func=self.get_path_info,
                description="Get detailed information about a specific path (file or directory). Input should be the path."
            ),
            Tool(
                name="explore_directory",
                func=self.explore_directory,
                description="Explore a directory and provide detailed analysis of its contents. Input should be the directory path."
            ),
            Tool(
                name="cat",
                func=self.cat_tool,
                description="Display the contents of a text file using bash cat"
            )
        ]

        return tools
