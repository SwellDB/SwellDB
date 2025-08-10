import os
import json
from pathlib import Path
from typing import Optional

class Config:
    def __init__(self, config_file: str = "config/swelldb_config.json"):
        # Get the SwellDB project root directory (where the config folder is located)
        current_file = Path(__file__)
        swelldb_root = current_file.parent.parent.parent  # Go up from util/ to swelldb/ to project root
        self.config_file = swelldb_root / config_file
        self._config = self._load_config()
    
    def _load_config(self) -> dict:
        """Load configuration from file if it exists"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}
    
    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get config value with environment variable override"""
        # Environment variable takes precedence
        env_value = os.getenv(key)
        if env_value:
            return env_value
        
        # Fall back to config file
        return self._config.get(key, default)
    
    def get_serper_api_key(self) -> Optional[str]:
        """Get SERPER_API_KEY with fallback to config file"""
        return self.get("SERPER_API_KEY")
    
    def get_openai_api_key(self) -> Optional[str]:
        """Get OPENAI_API_KEY with fallback to config file"""
        return self.get("OPENAI_API_KEY")
    
    def set(self, key: str, value: str):
        """Set a config value in the file"""
        self._config[key] = value
        self._save_config()
    
    def _save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self._config, f, indent=2)
        except IOError:
            pass  # Silently fail if we can't write config
