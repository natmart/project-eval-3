"""
Configuration module for PyTaskQ.

This module provides a Config class that loads configuration from YAML files,
supports environment variable overrides, and validates configuration values.
"""

import os
import os.path
from typing import Any, Dict, Optional

import yaml


class Config:
    """
    Configuration class for PyTaskQ.

    Loads configuration from YAML files and supports environment variable overrides.
    Validates configuration values and provides sensible defaults.

    Example:
        >>> config = Config.from_file("config.yaml")
        >>> max_workers = config.get("max_workers", 4)
    """

    # Default configuration values
    DEFAULT_CONFIG = {
        "max_workers": 4,
        "queue_size": 1000,
        "worker_timeout": 300,
        "task_timeout": 60,
        "retry_attempts": 3,
        "log_level": "INFO",
        "heartbeat_interval": 60,
    }

    # Valid log levels
    VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize a Config instance.

        Args:
            config_dict: Optional dictionary of configuration values.
                         If None, uses default configuration.
        """
        self._config = self.DEFAULT_CONFIG.copy()
        if config_dict is not None:
            self._config.update(config_dict)

    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        """
        Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file.

        Returns:
            A new Config instance.

        Raises:
            FileNotFoundError: If the configuration file doesn't exist.
            yaml.YAMLError: If the YAML file is malformed.
            ValueError: If configuration values are invalid.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, "r") as f:
            try:
                config_dict = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Invalid YAML in configuration file: {e}")

        if config_dict is None:
            config_dict = {}

        if not isinstance(config_dict, dict):
            raise ValueError(
                f"Configuration file must contain a dictionary, got {type(config_dict).__name__}"
            )

        return cls(config_dict)

    @classmethod
    def from_env(cls) -> "Config":
        """
        Load configuration from environment variables.

        Environment variables should be prefixed with PYTASKQ_:
        - PYTASKQ_MAX_WORKERS
        - PYTASKQ_QUEUE_SIZE
        - PYTASKQ_WORKER_TIMEOUT
        - PYTASKQ_TASK_TIMEOUT
        - PYTASKQ_RETRY_ATTEMPTS
        - PYTASKQ_LOG_LEVEL
        - PYTASKQ_HEARTBEAT_INTERVAL

        Returns:
            A new Config instance.
        """
        config_dict = {}

        # Map environment variable names to config keys
        env_mappings = {
            "PYTASKQ_MAX_WORKERS": "max_workers",
            "PYTASKQ_QUEUE_SIZE": "queue_size",
            "PYTASKQ_WORKER_TIMEOUT": "worker_timeout",
            "PYTASKQ_TASK_TIMEOUT": "task_timeout",
            "PYTASKQ_RETRY_ATTEMPTS": "retry_attempts",
            "PYTASKQ_LOG_LEVEL": "log_level",
            "PYTASKQ_HEARTBEAT_INTERVAL": "heartbeat_interval",
        }

        for env_var, config_key in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Convert to appropriate type
                config_dict[config_key] = cls._convert_env_value(config_key, value)

        return cls(config_dict)

    @classmethod
    def from_file_with_env_override(cls, file_path: str) -> "Config":
        """
        Load configuration from a YAML file and override with environment variables.

        Args:
            file_path: Path to the YAML configuration file.

        Returns:
            A new Config instance with environment variables merged.
        """
        config = cls.from_file(file_path)
        env_config = cls.from_env()

        # Merge environment config over file config
        for key, value in env_config._config.items():
            config.set(key, value)

        return config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            key: Configuration key.
            default: Default value if key is not found.

        Returns:
            The configuration value or default.
        """
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            key: Configuration key.
            value: Configuration value.
        """
        self._config[key] = value

    def validate(self) -> bool:
        """
        Validate all configuration values.

        Returns:
            True if all values are valid.

        Raises:
            ValueError: If any configuration value is invalid.
        """
        self._validate_max_workers(self.get("max_workers"))
        self._validate_queue_size(self.get("queue_size"))
        self._validate_worker_timeout(self.get("worker_timeout"))
        self._validate_task_timeout(self.get("task_timeout"))
        self._validate_retry_attempts(self.get("retry_attempts"))
        self._validate_log_level(self.get("log_level"))
        self._validate_heartbeat_interval(self.get("heartbeat_interval"))

        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        return self._config.copy()

    @staticmethod
    def _convert_env_value(key: str, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Numeric values
        if key in [
            "max_workers",
            "queue_size",
            "worker_timeout",
            "task_timeout",
            "retry_attempts",
            "heartbeat_interval",
        ]:
            return int(value)
        # String values
        return str(value)

    def _validate_max_workers(self, value: Any) -> None:
        """Validate max_workers value."""
        if not isinstance(value, int):
            raise ValueError(
                f"max_workers must be an integer, got {type(value).__name__}"
            )
        if value < 1:
            raise ValueError(f"max_workers must be at least 1, got {value}")
        if value > 100:
            raise ValueError(f"max_workers must be at most 100, got {value}")

    def _validate_queue_size(self, value: Any) -> None:
        """Validate queue_size value."""
        if not isinstance(value, int):
            raise ValueError(
                f"queue_size must be an integer, got {type(value).__name__}"
            )
        if value < 1:
            raise ValueError(f"queue_size must be at least 1, got {value}")
        if value > 100000:
            raise ValueError(f"queue_size must be at most 100000, got {value}")

    def _validate_worker_timeout(self, value: Any) -> None:
        """Validate worker_timeout value."""
        if not isinstance(value, int):
            raise ValueError(
                f"worker_timeout must be an integer, got {type(value).__name__}"
            )
        if value < 1:
            raise ValueError(f"worker_timeout must be at least 1, got {value}")

    def _validate_task_timeout(self, value: Any) -> None:
        """Validate task_timeout value."""
        if not isinstance(value, int):
            raise ValueError(
                f"task_timeout must be an integer, got {type(value).__name__}"
            )
        if value < 1:
            raise ValueError(f"task_timeout must be at least 1, got {value}")

    def _validate_retry_attempts(self, value: Any) -> None:
        """Validate retry_attempts value."""
        if not isinstance(value, int):
            raise ValueError(
                f"retry_attempts must be an integer, got {type(value).__name__}"
            )
        if value < 0:
            raise ValueError(f"retry_attempts must be non-negative, got {value}")

    def _validate_log_level(self, value: Any) -> None:
        """Validate log_level value."""
        if not isinstance(value, str):
            raise ValueError(
                f"log_level must be a string, got {type(value).__name__}"
            )
        if value.upper() not in self.VALID_LOG_LEVELS:
            raise ValueError(
                f"log_level must be one of {self.VALID_LOG_LEVELS}, got {value}"
            )

    def _validate_heartbeat_interval(self, value: Any) -> None:
        """Validate heartbeat_interval value."""
        if not isinstance(value, int):
            raise ValueError(
                f"heartbeat_interval must be an integer, got {type(value).__name__}"
            )
        if value < 1:
            raise ValueError(f"heartbeat_interval must be at least 1, got {value}")

    def __repr__(self) -> str:
        """String representation of Config."""
        return f"Config({self._config})"

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access."""
        return self._config[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Allow dict-like assignment."""
        self._config[key] = value

    def __contains__(self, key: str) -> bool:
        """Allow 'in' operator."""
        return key in self._config