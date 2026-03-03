"""
Tests for Configuration Module
"""

import os
import tempfile
import pytest
import yaml
from pytaskq.config import Config, ConfigError


class TestConfigDefaults:
    """Test default configuration values."""
    
    def test_default_values(self):
        """Test that default values are correctly set."""
        config = Config()
        
        assert config.worker_count == 4
        assert config.max_retries == 3
        assert config.storage_path == "./task_queue.db"
        assert config.log_level == "INFO"
    
    def test_defaults_dict_access(self):
        """Test accessing defaults via dictionary syntax."""
        config = Config()
        
        assert config["worker_count"] == 4
        assert config["max_retries"] == 3
        assert config["storage_path"] == "./task_queue.db"
        assert config["log_level"] == "INFO"


class TestConfigYAMLLoading:
    """Test loading configuration from YAML files."""
    
    def test_load_valid_yaml(self):
        """Test loading a valid YAML configuration file."""
        yaml_content = """
worker_count: 8
max_retries: 5
storage_path: /tmp/tasks.db
log_level: DEBUG
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config = Config(temp_path)
            
            assert config.worker_count == 8
            assert config.max_retries == 5
            assert config.storage_path == "/tmp/tasks.db"
            assert config.log_level == "DEBUG"
        finally:
            os.unlink(temp_path)
    
    def test_load_partial_yaml(self):
        """Test loading YAML with only some values specified."""
        yaml_content = """
worker_count: 10
log_level: ERROR
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config = Config(temp_path)
            
            assert config.worker_count == 10
            assert config.max_retries == 3  # Default value
            assert config.storage_path == "./task_queue.db"  # Default value
            assert config.log_level == "ERROR"
        finally:
            os.unlink(temp_path)
    
    def test_load_empty_yaml(self):
        """Test loading an empty YAML file uses defaults."""
        yaml_content = ""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config = Config(temp_path)
            
            assert config.worker_count == 4
            assert config.max_retries == 3
            assert config.storage_path == "./task_queue.db"
            assert config.log_level == "INFO"
        finally:
            os.unlink(temp_path)
    
    def test_load_yaml_with_null(self):
        """Test loading YAML with null content uses defaults."""
        yaml_content = "null"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config = Config(temp_path)
            
            # Should use all defaults
            assert config.worker_count == 4
            assert config.max_retries == 3
            assert config.storage_path == "./task_queue.db"
            assert config.log_level == "INFO"
        finally:
            os.unlink(temp_path)
    
    def test_file_not_found(self):
        """Test that requesting a non-existent file raises ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            Config("/nonexistent/path/to/config.yaml")
        
        assert "not found" in str(exc_info.value)
    
    def test_invalid_yaml(self):
        """Test that invalid YAML raises ConfigError."""
        yaml_content = """
worker_count: [invalid, list]
not: valid: yaml
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            with pytest.raises(ConfigError) as exc_info:
                Config(temp_path)
            
            assert "parsing" in str(exc_info.value).lower()
        finally:
            os.unlink(temp_path)
    
    def test_yaml_not_dict(self):
        """Test that YAML with non-dict root raises ConfigError."""
        yaml_content = """
- item1
- item2
- item3
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            with pytest.raises(ConfigError) as exc_info:
                Config(temp_path)
            
            assert "dictionary" in str(exc_info.value)
        finally:
            os.unlink(temp_path)


class TestConfigEnvironmentOverrides:
    """Test environment variable overrides."""
    
    def test_env_override_worker_count(self):
        """Test overriding worker_count via environment variable."""
        os.environ['PYTASKQ_WORKER_COUNT'] = '12'
        
        try:
            config = Config()
            assert config.worker_count == 12
        finally:
            del os.environ['PYTASKQ_WORKER_COUNT']
    
    def test_env_override_max_retries(self):
        """Test overriding max_retries via environment variable."""
        os.environ['PYTASKQ_MAX_RETRIES'] = '7'
        
        try:
            config = Config()
            assert config.max_retries == 7
        finally:
            del os.environ['PYTASKQ_MAX_RETRIES']
    
    def test_env_override_storage_path(self):
        """Test overriding storage_path via environment variable."""
        os.environ['PYTASKQ_STORAGE_PATH'] = '/custom/path/queue.db'
        
        try:
            config = Config()
            assert config.storage_path == '/custom/path/queue.db'
        finally:
            del os.environ['PYTASKQ_STORAGE_PATH']
    
    def test_env_override_log_level(self):
        """Test overriding log_level via environment variable."""
        os.environ['PYTASKQ_LOG_LEVEL'] = 'warning'
        
        try:
            config = Config()
            assert config.log_level == 'WARNING'  # Should be uppercase
        finally:
            del os.environ['PYTASKQ_LOG_LEVEL']
    
    def test_env_overrides_yaml(self):
        """Test that environment variables override YAML values."""
        yaml_content = """
worker_count: 2
storage_path: /yaml/path.db
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        os.environ['PYTASKQ_WORKER_COUNT'] = '20'
        os.environ['PYTASKQ_STORAGE_PATH'] = '/env/path.db'
        
        try:
            config = Config(temp_path)
            
            # Environment should override YAML
            assert config.worker_count == 20
            assert config.storage_path == '/env/path.db'
        finally:
            del os.environ['PYTASKQ_WORKER_COUNT']
            del os.environ['PYTASKQ_STORAGE_PATH']
            os.unlink(temp_path)
    
    def test_env_invalid_integer(self):
        """Test that invalid integer in environment raises ConfigError."""
        os.environ['PYTASKQ_WORKER_COUNT'] = 'not_a_number'
        
        try:
            with pytest.raises(ConfigError) as exc_info:
                Config()
            
            assert "integer" in str(exc_info.value).lower()
        finally:
            del os.environ['PYTASKQ_WORKER_COUNT']


class TestConfigValidation:
    """Test configuration validation."""
    
    def test_valid_worker_count(self):
        """Validate valid worker_count values."""
        config = Config()
        
        # Valid values
        config["worker_count"] = 1
        assert config.worker_count == 1
        
        config["worker_count"] = 50
        assert config.worker_count == 50
        
        config["worker_count"] = 100
        assert config.worker_count == 100
    
    def test_invalid_worker_count_type(self):
        """Test that non-integer worker_count raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["worker_count"] = "not_int"
        
        assert "worker_count" in str(exc_info.value)
        assert "integer" in str(exc_info.value)
    
    def test_invalid_worker_count_too_low(self):
        """Test that worker_count < 1 raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["worker_count"] = 0
        
        assert "worker_count" in str(exc_info.value)
        assert "at least 1" in str(exc_info.value)
    
    def test_invalid_worker_count_too_high(self):
        """Test that worker_count > 100 raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["worker_count"] = 101
        
        assert "worker_count" in str(exc_info.value)
        assert "exceed 100" in str(exc_info.value)
    
    def test_valid_max_retries(self):
        """Validate valid max_retries values."""
        config = Config()
        
        # Valid values
        config["max_retries"] = 0
        assert config.max_retries == 0
        
        config["max_retries"] = 5
        assert config.max_retries == 5
        
        config["max_retries"] = 10
        assert config.max_retries == 10
    
    def test_invalid_max_retries_type(self):
        """Test that non-integer max_retries raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["max_retries"] = "not_int"
        
        assert "max_retries" in str(exc_info.value)
        assert "integer" in str(exc_info.value)
    
    def test_invalid_max_retries_negative(self):
        """Test that negative max_retries raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["max_retries"] = -1
        
        assert "max_retries" in str(exc_info.value)
        assert "non-negative" in str(exc_info.value)
    
    def test_invalid_max_retries_too_high(self):
        """Test that max_retries > 10 raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["max_retries"] = 11
        
        assert "max_retries" in str(exc_info.value)
        assert "exceed 10" in str(exc_info.value)
    
    def test_valid_storage_path(self):
        """Validate valid storage_path values."""
        config = Config()
        
        # Valid values
        config["storage_path"] = "/path/to/file.db"
        assert config.storage_path == "/path/to/file.db"
        
        config["storage_path"] = "relative/path.db"
        assert config.storage_path == "relative/path.db"
    
    def test_invalid_storage_path_type(self):
        """Test that non-string storage_path raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["storage_path"] = 123
        
        assert "storage_path" in str(exc_info.value)
        assert "string" in str(exc_info.value)
    
    def test_invalid_storage_path_empty(self):
        """Test that empty storage_path raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["storage_path"] = ""
        
        assert "storage_path" in str(exc_info.value)
        assert "empty" in str(exc_info.value)
    
    def test_valid_log_levels(self):
        """Validate all valid log levels."""
        config = Config()
        
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config["log_level"] = level
            assert config.log_level == level
    
    def test_invalid_log_level_type(self):
        """Test that non-string log_level raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["log_level"] = 123
        
        assert "log_level" in str(exc_info.value)
        assert "string" in str(exc_info.value)
    
    def test_invalid_log_level_value(self):
        """Test that invalid log_level raises ConfigError."""
        config = Config()
        
        with pytest.raises(ConfigError) as exc_info:
            config["log_level"] = "INVALID"
        
        assert "log_level" in str(exc_info.value)
    
    def test_log_level_case_insensitive(self):
        """Test that log_level is case-insensitive but stored as uppercase."""
        config = Config()
        
        config["log_level"] = "debug"
        assert config.log_level == "DEBUG"
        
        config["log_level"] = "Info"
        assert config.log_level == "INFO"
        
        config["log_level"] = "warning"
        assert config.log_level == "WARNING"


class TestConfigDotNotation:
    """Test dot notation access."""
    
    def test_get_attribute(self):
        """Test getting values via dot notation."""
        config = Config()
        
        assert config.worker_count == 4
        assert config.max_retries == 3
        assert config.storage_path == "./task_queue.db"
        assert config.log_level == "INFO"
    
    def test_set_attribute(self):
        """Test setting values via dot notation."""
        config = Config()
        
        config.worker_count = 15
        assert config.worker_count == 15
        
        config.max_retries = 7
        assert config.max_retries == 7
        
        config.storage_path = "/new/path.db"
        assert config.storage_path == "/new/path.db"
    
    def test_invalid_attribute(self):
        """Test that accessing invalid attribute raises AttributeError."""
        config = Config()
        
        with pytest.raises(AttributeError):
            _ = config.nonexistent_key
    
    def test_private_attribute_access(self):
        """Test that private attributes work correctly."""
        config = Config()
        
        # These should not raise AttributeError
        assert hasattr(config, '_config')
        assert hasattr(config, 'DEFAULTS')


class TestConfigDictMethods:
    """Test dict-like methods."""
    
    def test_get_method(self):
        """Test the get() method."""
        config = Config()
        
        assert config.get("worker_count") == 4
        assert config.get("nonexistent", "default") == "default"
        assert config.get("nonexistent") is None
    
    def test_keys_method(self):
        """Test the keys() method."""
        config = Config()
        
        keys = list(config.keys())
        assert "worker_count" in keys
        assert "max_retries" in keys
        assert "storage_path" in keys
        assert "log_level" in keys
    
    def test_values_method(self):
        """Test the values() method."""
        config = Config()
        
        values = list(config.values())
        assert 4 in values
        assert 3 in values
        assert "./task_queue.db" in values
        assert "INFO" in values
    
    def test_items_method(self):
        """Test the items() method."""
        config = Config()
        
        items = dict(config.items())
        assert items["worker_count"] == 4
        assert items["max_retries"] == 3
        assert items["storage_path"] == "./task_queue.db"
        assert items["log_level"] == "INFO"
    
    def test_to_dict(self):
        """Test the to_dict() method."""
        config = Config()
        
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert config_dict["worker_count"] == 4
        assert config_dict["max_retries"] == 3
        assert config_dict["storage_path"] == "./task_queue.db"
        assert config_dict["log_level"] == "INFO"
        
        # Ensure it's a copy, not the same object
        config_dict["worker_count"] = 100
        assert config.worker_count == 4
    
    def test_contains_method(self):
        """Test the __contains__ method."""
        config = Config()
        
        assert "worker_count" in config
        assert "max_retries" in config
        assert "nonexistent" not in config


class TestConfigRepresentation:
    """Test configuration representation."""
    
    def test_repr(self):
        """Test the __repr__ method."""
        config = Config()
        
        repr_str = repr(config)
        
        assert "Config" in repr_str
        assert "worker_count" in repr_str
        assert "4" in repr_str


class TestConfigHelperMethods:
    """Test helper methods."""
    
    def test_get_log_level_value(self):
        """Test the get_log_level_value class method."""
        import logging
        
        assert Config.get_log_level_value("DEBUG") == logging.DEBUG
        assert Config.get_log_level_value("INFO") == logging.INFO
        assert Config.get_log_level_value("WARNING") == logging.WARNING
        assert Config.get_log_level_value("ERROR") == logging.ERROR
        assert Config.get_log_level_value("CRITICAL") == logging.CRITICAL
        assert Config.get_log_level_value("invalid") == logging.INFO  # Default


class TestConfigIntegration:
    """Integration tests with real-world scenarios."""
    
    def test_full_loading_chain(self):
        """Test the full chain: defaults -> YAML -> env -> validation."""
        # Create a YAML file
        yaml_content = """
worker_count: 6
max_retries: 2
storage_path: /yaml/path.db
log_level: debug
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        # Set environment variables
        os.environ['PYTASKQ_WORKER_COUNT'] = '8'
        os.environ['PYTASKQ_STORAGE_PATH'] = '/env/path.db'
        
        try:
            config = Config(temp_path)
            
            # Defaults: worker_count=4, max_retries=3, storage_path="./task_queue.db", log_level="INFO"
            # YAML overrides: worker_count=6, max_retries=2, storage_path="/yaml/path.db", log_level="debug"
            # Env overrides: worker_count=8, storage_path="/env/path.db"
            
            assert config.worker_count == 8  # From env (highest priority)
            assert config.max_retries == 2  # From YAML
            assert config.storage_path == '/env/path.db'  # From env
            assert config.log_level == 'DEBUG'  # From YAML (normalized to uppercase)
            
            # Test both access methods work
            assert config["worker_count"] == 8
            assert config.max_retries == 2
            
        finally:
            del os.environ['PYTASKQ_WORKER_COUNT']
            del os.environ['PYTASKQ_STORAGE_PATH']
            os.unlink(temp_path)
    
    def test_multiple_configs(self):
        """Test creating multiple independent config instances."""
        config1 = Config()
        
        yaml_content = "worker_count: 20"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config2 = Config(temp_path)
            
            # Configs should be independent
            config1.worker_count = 10
            config2.worker_count = 30
            
            assert config1.worker_count == 10
            assert config2.worker_count == 30
        finally:
            os.unlink(temp_path)