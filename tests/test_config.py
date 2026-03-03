"""
Unit tests for the Config class.
"""

import os
import tempfile

import pytest
import yaml

from pytaskq.config import Config


class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_config_values(self):
        """Test that default configuration values are set correctly."""
        config = Config()
        assert config.get("max_workers") == 4
        assert config.get("queue_size") == 1000
        assert config.get("worker_timeout") == 300
        assert config.get("task_timeout") == 60
        assert config.get("retry_attempts") == 3
        assert config.get("log_level") == "INFO"
        assert config.get("heartbeat_interval") == 60

    def test_default_config_is_immutable_original(self):
        """Test that modifying config doesn't affect class defaults."""
        config1 = Config()
        config1.set("max_workers", 10)
        assert config1.get("max_workers") == 10

        config2 = Config()
        assert config2.get("max_workers") == 4  # Should still be default

    def test_get_with_default(self):
        """Test get() method with default values."""
        config = Config()
        assert config.get("nonexistent_key", "default") == "default"
        assert config.get("max_workers", 100) != 100  # Has actual value

    def test_get_without_default(self):
        """Test get() method without default values."""
        config = Config()
        assert config.get("max_workers") == 4
        assert config.get("nonexistent_key") is None


class TestConfigYAMLLoading:
    """Test YAML configuration file loading."""

    def test_load_from_valid_yaml(self):
        """Test loading configuration from a valid YAML file."""
        yaml_content = """
        max_workers: 8
        queue_size: 500
        log_level: DEBUG
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            assert config.get("max_workers") == 8
            assert config.get("queue_size") == 500
            assert config.get("log_level") == "DEBUG"
            # Default values for unspecified keys
            assert config.get("worker_timeout") == 300
            assert config.get("task_timeout") == 60
        finally:
            os.unlink(temp_path)

    def test_load_from_empty_yaml(self):
        """Test loading configuration from an empty YAML file."""
        yaml_content = ""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            # Should have all default values
            assert config.get("max_workers") == 4
            assert config.get("queue_size") == 1000
        finally:
            os.unlink(temp_path)

    def test_load_from_yaml_with_all_values(self):
        """Test loading YAML with all configuration values."""
        yaml_content = """
        max_workers: 16
        queue_size: 2000
        worker_timeout: 600
        task_timeout: 120
        retry_attempts: 5
        log_level: WARNING
        heartbeat_interval: 120
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            assert config.get("max_workers") == 16
            assert config.get("queue_size") == 2000
            assert config.get("worker_timeout") == 600
            assert config.get("task_timeout") == 120
            assert config.get("retry_attempts") == 5
            assert config.get("log_level") == "WARNING"
            assert config.get("heartbeat_interval") == 120
        finally:
            os.unlink(temp_path)

    def test_load_from_nonexistent_file(self):
        """Test loading configuration from a non-existent file."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            Config.from_file("/nonexistent/path/config.yaml")

    def test_load_from_malformed_yaml(self):
        """Test loading configuration from a malformed YAML file."""
        yaml_content = """
        max_workers: [unclosed list
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(yaml.YAMLError, match="Invalid YAML"):
                Config.from_file(temp_path)
        finally:
            os.unlink(temp_path)

    def test_load_from_non_dict_yaml(self):
        """Test loading YAML that doesn't contain a dictionary."""
        yaml_content = """
        - item1
        - item2
        - item3
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="must contain a dictionary"):
                Config.from_file(temp_path)
        finally:
            os.unlink(temp_path)


class TestConfigValidation:
    """Test configuration value validation."""

    def test_validate_valid_config(self):
        """Test validating a valid configuration."""
        config = Config()
        assert config.validate() is True

    def test_validate_max_workers_valid(self):
        """Test valid max_workers values."""
        config = Config()
        for value in [1, 4, 10, 50, 100]:
            config.set("max_workers", value)
            assert config.validate() is True

    def test_validate_max_workers_invalid_type(self):
        """Test max_workers with invalid type."""
        config = Config()
        config.set("max_workers", "4")
        with pytest.raises(ValueError, match="max_workers must be an integer"):
            config.validate()

    def test_validate_max_workers_too_low(self):
        """Test max_workers with value too low."""
        config = Config()
        config.set("max_workers", 0)
        with pytest.raises(ValueError, match="max_workers must be at least 1"):
            config.validate()

    def test_validate_max_workers_too_high(self):
        """Test max_workers with value too high."""
        config = Config()
        config.set("max_workers", 101)
        with pytest.raises(ValueError, match="max_workers must be at most 100"):
            config.validate()

    def test_validate_queue_size_valid(self):
        """Test valid queue_size values."""
        config = Config()
        for value in [1, 100, 1000, 10000]:
            config.set("queue_size", value)
            assert config.validate() is True

    def test_validate_queue_size_invalid_type(self):
        """Test queue_size with invalid type."""
        config = Config()
        config.set("queue_size", "1000")
        with pytest.raises(ValueError, match="queue_size must be an integer"):
            config.validate()

    def test_validate_queue_size_too_low(self):
        """Test queue_size with value too low."""
        config = Config()
        config.set("queue_size", 0)
        with pytest.raises(ValueError, match="queue_size must be at least 1"):
            config.validate()

    def test_validate_queue_size_too_high(self):
        """Test queue_size with value too high."""
        config = Config()
        config.set("queue_size", 100001)
        with pytest.raises(ValueError, match="queue_size must be at most 100000"):
            config.validate()

    def test_validate_worker_timeout_valid(self):
        """Test valid worker_timeout values."""
        config = Config()
        for value in [1, 60, 300, 600]:
            config.set("worker_timeout", value)
            assert config.validate() is True

    def test_validate_worker_timeout_invalid(self):
        """Test worker_timeout with invalid value."""
        config = Config()
        config.set("worker_timeout", 0)
        with pytest.raises(ValueError, match="worker_timeout must be at least 1"):
            config.validate()

    def test_validate_task_timeout_valid(self):
        """Test valid task_timeout values."""
        config = Config()
        for value in [1, 30, 60, 120]:
            config.set("task_timeout", value)
            assert config.validate() is True

    def test_validate_task_timeout_invalid(self):
        """Test task_timeout with invalid value."""
        config = Config()
        config.set("task_timeout", 0)
        with pytest.raises(ValueError, match="task_timeout must be at least 1"):
            config.validate()

    def test_validate_retry_attempts_valid(self):
        """Test valid retry_attempts values."""
        config = Config()
        for value in [0, 1, 3, 10]:
            config.set("retry_attempts", value)
            assert config.validate() is True

    def test_validate_retry_attempts_invalid_type(self):
        """Test retry_attempts with invalid type."""
        config = Config()
        config.set("retry_attempts", "3")
        with pytest.raises(ValueError, match="retry_attempts must be an integer"):
            config.validate()

    def test_validate_retry_attempts_negative(self):
        """Test retry_attempts with negative value."""
        config = Config()
        config.set("retry_attempts", -1)
        with pytest.raises(ValueError, match="retry_attempts must be non-negative"):
            config.validate()

    def test_validate_log_level_valid(self):
        """Test valid log_level values."""
        config = Config()
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config.set("log_level", level)
            assert config.validate() is True

    def test_validate_log_level_case_insensitive(self):
        """Test log_level accepts different cases."""
        config = Config()
        config.set("log_level", "debug")
        assert config.validate() is True
        config.set("log_level", "Info")
        assert config.validate() is True

    def test_validate_log_level_invalid_type(self):
        """Test log_level with invalid type."""
        config = Config()
        config.set("log_level", 123)
        with pytest.raises(ValueError, match="log_level must be a string"):
            config.validate()

    def test_validate_log_level_invalid_value(self):
        """Test log_level with invalid value."""
        config = Config()
        config.set("log_level", "INVALID")
        with pytest.raises(ValueError, match="log_level must be one of"):
            config.validate()

    def test_validate_heartbeat_interval_valid(self):
        """Test valid heartbeat_interval values."""
        config = Config()
        for value in [1, 30, 60, 300]:
            config.set("heartbeat_interval", value)
            assert config.validate() is True

    def test_validate_heartbeat_interval_invalid(self):
        """Test heartbeat_interval with invalid value."""
        config = Config()
        config.set("heartbeat_interval", 0)
        with pytest.raises(ValueError, match="heartbeat_interval must be at least 1"):
            config.validate()


class TestConfigEnvironmentOverrides:
    """Test environment variable overrides."""

    def test_from_env_no_env_vars(self):
        """Test loading config from environment with no variables set."""
        # Clear any existing PYTASKQ_ variables
        env_vars = [k for k in os.environ if k.startswith("PYTASKQ_")]
        for var in env_vars:
            del os.environ[var]

        config = Config.from_env()
        # Should have default values
        assert config.get("max_workers") == 4
        assert config.get("queue_size") == 1000

    def test_from_env_with_numeric_vars(self):
        """Test loading config with numeric environment variables."""
        os.environ["PYTASKQ_MAX_WORKERS"] = "10"
        os.environ["PYTASKQ_QUEUE_SIZE"] = "5000"
        os.environ["PYTASKQ_WORKER_TIMEOUT"] = "600"

        try:
            config = Config.from_env()
            assert config.get("max_workers") == 10
            assert config.get("queue_size") == 5000
            assert config.get("worker_timeout") == 600
            # Other values should be defaults
            assert config.get("task_timeout") == 60
        finally:
            os.environ.pop("PYTASKQ_MAX_WORKERS", None)
            os.environ.pop("PYTASKQ_QUEUE_SIZE", None)
            os.environ.pop("PYTASKQ_WORKER_TIMEOUT", None)

    def test_from_env_with_string_vars(self):
        """Test loading config with string environment variables."""
        os.environ["PYTASKQ_LOG_LEVEL"] = "DEBUG"

        try:
            config = Config.from_env()
            assert config.get("log_level") == "DEBUG"
        finally:
            os.environ.pop("PYTASKQ_LOG_LEVEL", None)

    def test_from_env_type_conversion(self):
        """Test that environment variables are converted to proper types."""
        os.environ["PYTASKQ_MAX_WORKERS"] = "8"
        os.environ["PYTASKQ_RETRY_ATTEMPTS"] = "5"

        try:
            config = Config.from_env()
            assert isinstance(config.get("max_workers"), int)
            assert isinstance(config.get("retry_attempts"), int)
            assert isinstance(config.get("log_level"), str)
        finally:
            os.environ.pop("PYTASKQ_MAX_WORKERS", None)
            os.environ.pop("PYTASKQ_RETRY_ATTEMPTS", None)

    def test_from_file_with_env_override(self):
        """Test loading config from file with environment override."""
        # Create a YAML file
        yaml_content = """
        max_workers: 4
        queue_size: 1000
        log_level: INFO
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        # Set environment variable to override
        os.environ["PYTASKQ_MAX_WORKERS"] = "10"
        os.environ["PYTASKQ_LOG_LEVEL"] = "DEBUG"

        try:
            config = Config.from_file_with_env_override(temp_path)
            # Environment should override file values
            assert config.get("max_workers") == 10
            assert config.get("log_level") == "DEBUG"
            # File values should be used for non-overridden keys
            assert config.get("queue_size") == 1000
        finally:
            os.environ.pop("PYTASKQ_MAX_WORKERS", None)
            os.environ.pop("PYTASKQ_LOG_LEVEL", None)
            os.unlink(temp_path)

    def test_env_override_numeric_validation(self):
        """Test that overridden values are still validated."""
        os.environ["PYTASKQ_MAX_WORKERS"] = "150"  # Too high

        try:
            config = Config.from_env()
            with pytest.raises(ValueError, match="max_workers must be at most 100"):
                config.validate()
        finally:
            os.environ.pop("PYTASKQ_MAX_WORKERS", None)


class TestConfigDictLikeAccess:
    """Test dictionary-like access to Config."""

    def test_getitem_access(self):
        """Test accessing config using [] notation."""
        config = Config()
        assert config["max_workers"] == 4
        assert config["log_level"] == "INFO"

    def test_setitem_access(self):
        """Test setting config using [] notation."""
        config = Config()
        config["max_workers"] = 10
        assert config["max_workers"] == 10
        assert config.get("max_workers") == 10

    def test_contains_operator(self):
        """Test using 'in' operator."""
        config = Config()
        assert "max_workers" in config
        assert "log_level" in config
        assert "nonexistent_key" not in config

    def test_getitem_key_not_found(self):
        """Test accessing non-existent key with [] notation."""
        config = Config()
        with pytest.raises(KeyError):
            _ = config["nonexistent_key"]


class TestConfigMethods:
    """Test Config methods."""

    def test_set_method(self):
        """Test set() method."""
        config = Config()
        config.set("max_workers", 20)
        assert config.get("max_workers") == 20

        # Set a new key
        config.set("custom_key", "custom_value")
        assert config.get("custom_key") == "custom_value"

    def test_to_dict(self):
        """Test to_dict() method."""
        config = Config()
        config_dict = config.to_dict()

        assert isinstance(config_dict, dict)
        assert config_dict["max_workers"] == 4
        assert config_dict["log_level"] == "INFO"
        assert len(config_dict) == 7  # Number of default keys

    def test_to_dict_returns_copy(self):
        """Test that to_dict() returns a copy, not reference."""
        config = Config()
        config_dict = config.to_dict()
        config_dict["max_workers"] = 100

        # Original config should not be affected
        assert config.get("max_workers") == 4

    def test_custom_config_init(self):
        """Test Config initialization with custom values."""
        custom_dict = {
            "max_workers": 8,
            "queue_size": 500,
            "log_level": "DEBUG",
        }
        config = Config(custom_dict)
        assert config.get("max_workers") == 8
        assert config.get("queue_size") == 500
        assert config.get("log_level") == "DEBUG"
        # Other values should be defaults
        assert config.get("worker_timeout") == 300
        assert config.get("task_timeout") == 60

    def test_repr(self):
        """Test string representation of Config."""
        config = Config()
        repr_str = repr(config)
        assert "Config(" in repr_str
        assert "max_workers" in repr_str


class TestConfigEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_multiple_configs_independent(self):
        """Test that multiple Config instances are independent."""
        config1 = Config()
        config2 = Config()

        config1.set("max_workers", 10)
        config2.set("max_workers", 20)

        assert config1.get("max_workers") == 10
        assert config2.get("max_workers") == 20

    def test_config_with_minimal_values(self):
        """Test config with minimal valid values."""
        config = Config()
        config.set("max_workers", 1)
        config.set("queue_size", 1)
        config.set("worker_timeout", 1)
        config.set("task_timeout", 1)
        config.set("retry_attempts", 0)
        config.set("heartbeat_interval", 1)

        assert config.validate() is True

    def test_config_with_maximal_values(self):
        """Test config with maximal valid values."""
        config = Config()
        config.set("max_workers", 100)
        config.set("queue_size", 100000)
        config.set("log_level", "CRITICAL")

        assert config.validate() is True

    def test_yaml_with_nested_structure(self):
        """Test YAML with nested structure (should use top-level keys)."""
        yaml_content = """
        max_workers: 10
        section:
          queue_size: 500
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            # Only top-level keys should be used
            assert config.get("max_workers") == 10
            # 'queue_size' should be default, not nested
            assert config.get("queue_size") == 1000
            assert config.get("section") is None
        finally:
            os.unlink(temp_path)

    def test_yaml_with_comments(self):
        """Test YAML with comments (should work fine)."""
        yaml_content = """
        # This is a comment
        max_workers: 8
        # Another comment
        log_level: DEBUG
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            assert config.get("max_workers") == 8
            assert config.get("log_level") == "DEBUG"
        finally:
            os.unlink(temp_path)


class TestConfigValidLogLevels:
    """Test valid log levels constant."""

    def test_valid_log_levels(self):
        """Test that VALID_LOG_LEVELS contains expected values."""
        expected = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        assert Config.VALID_LOG_LEVELS == expected

    def test_log_level_validation_uses_constant(self):
        """Test that log level validation uses VALID_LOG_LEVELS."""
        config = Config()
        for level in Config.VALID_LOG_LEVELS:
            config.set("log_level", level)
            config.validate()  # Should not raise

        # Test one invalid value
        config.set("log_level", "NOTVALID")
        with pytest.raises(ValueError):
            config.validate()