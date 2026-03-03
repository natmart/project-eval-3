"""
Configuration Module

This module provides the Config class for loading and managing YAML-based
configuration with environment variable overrides and validation.
"""

import os
import logging
from typing import Any, Dict, Optional
import yaml


class ConfigError(Exception):
    """Exception raised for configuration errors."""
    pass


class Config:
    """
    Configuration class for loading YAML configuration files.
    
    Supports:
    - Loading from YAML files
    - Default values
    - Validation of config values
    - Environment variable overrides
    - Dot notation and dict access
    """
    
    # Default configuration values
    DEFAULTS: Dict[str, Any] = {
        "worker_count": 4,
        "max_retries": 3,
        "storage_path": "./task_queue.db",
        "log_level": "INFO",
    }
    
    # Valid log levels
    VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    
    # Environment variable prefix
    ENV_PREFIX = "PYTASKQ_"
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the Config instance.
        
        Args:
            config_path: Optional path to YAML configuration file.
                        If None, uses default values only.
        """
        self._config: Dict[str, Any] = self.DEFAULTS.copy()
        
        if config_path:
            self._load_from_file(config_path)
        
        self._override_from_env()
        self._validate()
    
    def _load_from_file(self, config_path: str) -> None:
        """
        Load configuration from a YAML file.
        
        Args:
            config_path: Path to YAML configuration file.
            
        Raises:
            ConfigError: If the file cannot be read or parsed.
        """
        try:
            with open(config_path, 'r') as f:
                yaml_config = yaml.safe_load(f)
                
                if yaml_config is None:
                    return
                
                if not isinstance(yaml_config, dict):
                    raise ConfigError(
                        f"Configuration file must contain a dictionary, "
                        f"got {type(yaml_config).__name__}"
                    )
                
                # Update config with values from YAML file
                self._config.update(yaml_config)
                
        except FileNotFoundError:
            raise ConfigError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigError(f"Error parsing YAML file: {e}")
        except Exception as e:
            raise ConfigError(f"Error reading configuration file: {e}")
    
    def _override_from_env(self) -> None:
        """Override configuration values from environment variables."""
        for key in self._config.keys():
            env_key = f"{self.ENV_PREFIX}{key.upper()}"
            env_value = os.environ.get(env_key)
            
            if env_value is not None:
                # Convert environment variable to appropriate type
                self._config[key] = self._convert_env_value(env_value, key)
    
    def _convert_env_value(self, value: str, key: str) -> Any:
        """
        Convert environment variable string to appropriate type.
        
        Args:
            value: String value from environment variable.
            key: Configuration key to determine type.
            
        Returns:
            Converted value.
        """
        # Get default value to infer type
        default = self.DEFAULTS.get(key)
        
        if isinstance(default, int):
            try:
                return int(value)
            except ValueError:
                raise ConfigError(
                    f"Invalid integer value for {key}: {value}"
                )
        elif isinstance(default, str):
            return value
        
        return value
    
    def _validate(self) -> None:
        """
        Validate configuration values.
        
        Raises:
            ConfigError: If any configuration value is invalid.
        """
        # Validate worker_count
        if not isinstance(self._config.get("worker_count"), int):
            raise ConfigError("worker_count must be an integer")
        if self._config["worker_count"] < 1:
            raise ConfigError("worker_count must be at least 1")
        if self._config["worker_count"] > 100:
            raise ConfigError("worker_count must not exceed 100")
        
        # Validate max_retries
        if not isinstance(self._config.get("max_retries"), int):
            raise ConfigError("max_retries must be an integer")
        if self._config["max_retries"] < 0:
            raise ConfigError("max_retries must be non-negative")
        if self._config["max_retries"] > 10:
            raise ConfigError("max_retries must not exceed 10")
        
        # Validate storage_path
        if not isinstance(self._config.get("storage_path"), str):
            raise ConfigError("storage_path must be a string")
        if not self._config["storage_path"]:
            raise ConfigError("storage_path cannot be empty")
        
        # Validate log_level
        if not isinstance(self._config.get("log_level"), str):
            raise ConfigError("log_level must be a string")
        log_level_upper = self._config["log_level"].upper()
        if log_level_upper not in self.VALID_LOG_LEVELS:
            raise ConfigError(
                f"log_level must be one of {self.VALID_LOG_LEVELS}, "
                f"got {self._config['log_level']}"
            )
        self._config["log_level"] = log_level_upper
    
    # Dict-like access
    def __getitem__(self, key: str) -> Any:
        """
        Get configuration value by key (dict access).
        
        Args:
            key: Configuration key.
            
        Returns:
            Configuration value.
            
        Raises:
            KeyError: If key is not found.
        """
        if key not in self._config:
            raise KeyError(f"Configuration key not found: {key}")
        return self._config[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set configuration value by key (dict access).
        
        Args:
            key: Configuration key.
            value: New value.
        """
        self._config[key] = value
        self._validate()
    
    def __contains__(self, key: str) -> bool:
        """
        Check if configuration key exists.
        
        Args:
            key: Configuration key.
            
        Returns:
            True if key exists, False otherwise.
        """
        return key in self._config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value with default.
        
        Args:
            key: Configuration key.
            default: Default value if key not found.
            
        Returns:
            Configuration value or default.
        """
        return self._config.get(key, default)
    
    def keys(self) -> Dict[str, Any]:
        """Return configuration keys."""
        return self._config.keys()
    
    def values(self) -> Dict[str, Any]:
        """Return configuration values."""
        return self._config.values()
    
    def items(self) -> Dict[str, Any]:
        """Return configuration items."""
        return self._config.items()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Return configuration as dictionary.
        
        Returns:
            Dictionary representation of configuration.
        """
        return self._config.copy()
    
    # Dot notation access
    def __getattr__(self, name: str) -> Any:
        """
        Get configuration value by attribute (dot notation).
        
        Args:
            name: Configuration key.
            
        Returns:
            Configuration value.
            
        Raises:
            AttributeError: If key is not found.
        """
        if name in self._config:
            return self._config[name]
        raise AttributeError(f"Configuration has no attribute '{name}'")
    
    def __setattr__(self, name: str, value: Any) -> None:
        """
        Set configuration value by attribute (dot notation).
        
        Args:
            name: Configuration key.
            value: New value.
        """
        # Don't override private attributes
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            self._config[name] = value
            self._validate()
    
    def __repr__(self) -> str:
        """Return string representation of configuration."""
        return f"Config({self._config})"
    
    @classmethod
    def get_log_level_value(cls, log_level: str) -> int:
        """
        Convert log level string to logging module constant.
        
        Args:
            log_level: Log level string (e.g., "INFO").
            
        Returns:
            Logging module constant (e.g., logging.INFO).
        """
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        return level_map.get(log_level.upper(), logging.INFO)