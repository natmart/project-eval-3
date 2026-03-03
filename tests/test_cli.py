"""
Unit tests for the CLI module.

Tests cover argument parsing, command execution, and integration with
the task queue components.
"""

import argparse
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call

import yaml


class TestArgumentParsing(unittest.TestCase):
    """Test CLI argument parsing."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import create_parser
        self.parser = create_parser()
    
    def test_parser_creation(self):
        """Test that parser is created successfully."""
        self.assertIsNotNone(self.parser)
    
    def test_version_flag(self):
        """Test version flag."""
        args = self.parser.parse_args(["--version"])
        self.assertTrue(args.version)
    
    def test_no_command(self):
        """Test parsing with no command."""
        with self.assertRaises(SystemExit):
            self.parser.parse_args([])
    
    def test_worker_command_basic(self):
        """Test basic worker command parsing."""
        args = self.parser.parse_args(["worker"])
        self.assertEqual(args.command, "worker")
        self.assertEqual(args.workers, None)
        self.assertEqual(args.config, None)
    
    def test_worker_command_with_options(self):
        """Test worker command with all options."""
        args = self.parser.parse_args([
            "worker",
            "--workers", "8",
            "--config", "test.yaml",
            "--storage", "test.db",
            "--metrics-interval", "30"
        ])
        self.assertEqual(args.command, "worker")
        self.assertEqual(args.workers, 8)
        self.assertEqual(args.config, "test.yaml")
        self.assertEqual(args.storage, "test.db")
        self.assertEqual(args.metrics_interval, 30)
    
    def test_submit_command_basic(self):
        """Test basic submit command parsing."""
        args = self.parser.parse_args(["submit", "test_task"])
        self.assertEqual(args.command, "submit")
        self.assertEqual(args.name, "test_task")
        self.assertEqual(args.priority, None)
    
    def test_submit_command_with_options(self):
        """Test submit command with all options."""
        args = self.parser.parse_args([
            "submit",
            "test_task",
            "--priority", "3",
            "--metadata", '{"key": "value"}',
            "--storage", "test.db"
        ])
        self.assertEqual(args.command, "submit")
        self.assertEqual(args.name, "test_task")
        self.assertEqual(args.priority, 3)
        self.assertEqual(args.metadata, '{"key": "value"}')
        self.assertEqual(args.storage, "test.db")
    
    def test_submit_invalid_priority(self):
        """Test submit command with invalid priority."""
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["submit", "test", "--priority", "15"])
    
    def test_status_command_basic(self):
        """Test basic status command parsing."""
        args = self.parser.parse_args(["status"])
        self.assertEqual(args.command, "status")
        self.assertEqual(args.storage, None)
    
    def test_status_with_storage(self):
        """Test status command with storage option."""
        args = self.parser.parse_args(["status", "--storage", "test.db"])
        self.assertEqual(args.command, "status")
        self.assertEqual(args.storage, "test.db")
    
    def test_config_show_action(self):
        """Test config show action parsing."""
        args = self.parser.parse_args(["config", "show"])
        self.assertEqual(args.command, "config")
        self.assertEqual(args.action, "show")
        self.assertEqual(args.env, False)
    
    def test_config_validate_action(self):
        """Test config validate action parsing."""
        args = self.parser.parse_args(["config", "validate", "--config", "test.yaml"])
        self.assertEqual(args.command, "config")
        self.assertEqual(args.action, "validate")
        self.assertEqual(args.config, "test.yaml")
        self.assertEqual(args.env, False)
    
    def test_config_generate_action(self):
        """Test config generate action parsing."""
        args = self.parser.parse_args([
            "config", "generate",
            "--output", "custom.yaml"
        ])
        self.assertEqual(args.command, "config")
        self.assertEqual(args.action, "generate")
        self.assertEqual(args.output, "custom.yaml")
    
    def test_config_env_flag(self):
        """Test config command with env flag."""
        args = self.parser.parse_args(["config", "show", "--env"])
        self.assertEqual(args.command, "config")
        self.assertEqual(args.action, "show")
        self.assertEqual(args.env, True)
    
    def test_metrics_command(self):
        """Test metrics command parsing."""
        args = self.parser.parse_args(["metrics"])
        self.assertEqual(args.command, "metrics")
    
    def test_version_command(self):
        """Test version command parsing."""
        args = self.parser.parse_args(["version"])
        self.assertEqual(args.command, "version")
    
    def test_log_level_option(self):
        """Test log level option."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            args = self.parser.parse_args(["--log-level", level, "status"])
            self.assertEqual(args.log_level, level)


class TestWorkerCommand(unittest.TestCase):
    """Test the worker command functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import cmd_worker
        self.cmd_worker = cmd_worker
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.Config')
    @patch('pytaskq.cli.TaskQueue')
    @patch('pytaskq.cli.Scheduler')
    @patch('pytaskq.cli.WorkerPool')
    @patch('pytaskq.cli.signal.signal')
    def test_worker_starts_successfully(
        self, mock_signal, mock_worker_pool, mock_scheduler,
        mock_task_queue, mock_config, mock_logging
    ):
        """Test that worker starts successfully with default parameters."""
        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config.from_env.return_value = mock_config_instance
        mock_config_instance.get.return_value = 4
        mock_config_instance.validate.return_value = True
        
        mock_queue_instance = MagicMock()
        mock_task_queue.return_value = mock_queue_instance
        
        mock_scheduler_instance = MagicMock()
        mock_scheduler.return_value = mock_scheduler_instance
        
        mock_worker_pool_instance = MagicMock()
        mock_worker_pool.return_value = mock_worker_pool_instance
        
        # Create args
        args = argparse.Namespace(
            workers=None,
            config=None,
            storage=None,
            metrics_interval=None,
            log_level="INFO"
        )
        
        # Run in a thread with timeout to avoid hanging
        def run_worker():
            self.cmd_worker(args)
        
        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()
        time.sleep(0.2)
        
        # Verify setup
        mock_config.from_env.assert_called_once()
        mock_scheduler.assert_called_once()
        mock_worker_pool.assert_called_once()
        mock_scheduler_instance.start.assert_called_once()
        mock_worker_pool_instance.start.assert_called_once()
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.Config.from_file')
    @patch('pytaskq.cli.SQLiteBackend')
    @patch('pytaskq.cli.TaskQueue')
    @patch('pytaskq.cli.Scheduler')
    @patch('pytaskq.cli.WorkerPool')
    def test_worker_with_config_file(
        self, mock_worker_pool, mock_scheduler, mock_task_queue,
        mock_storage, mock_config, mock_logging
    ):
        """Test worker loading config from file."""
        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.validate.return_value = True
        
        args = argparse.Namespace(
            workers=8,
            config="test.yaml",
            storage="test.db",
            metrics_interval=60,
            log_level="DEBUG"
        )
        
        def run_worker():
            self.cmd_worker(args)
        
        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()
        time.sleep(0.1)
        
        # Verify config was loaded from file
        mock_config.assert_called_once_with("test.yaml")
        mock_storage.assert_called_once_with("test.db")


class TestSubmitCommand(unittest.TestCase):
    """Test the submit command functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import cmd_submit
        self.cmd_submit = cmd_submit
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.TaskQueue')
    @patch('builtins.print')
    def test_submit_basic_task(self, mock_print, mock_task_queue, mock_logging):
        """Test submitting a basic task."""
        mock_queue_instance = MagicMock()
        mock_task_queue.return_value = mock_queue_instance
        
        args = argparse.Namespace(
            name="test_task",
            priority=None,
            metadata=None,
            storage=None,
            log_level="INFO"
        )
        
        self.cmd_submit(args)
        
        # Verify task was enqueued
        mock_queue_instance.enqueue.assert_called_once()
        task_arg = mock_queue_instance.enqueue.call_args[0][0]
        self.assertEqual(task_arg.name, "test_task")
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.TaskQueue')
    @patch('pytaskq.cli.SQLiteBackend')
    @patch('builtins.print')
    def test_submit_with_storage(self, mock_print, mock_storage, mock_task_queue, mock_logging):
        """Test submitting a task with storage."""
        mock_queue_instance = MagicMock()
        mock_task_queue.return_value = mock_queue_instance
        
        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance
        
        args = argparse.Namespace(
            name="test_task",
            priority=3,
            metadata='{"key": "value"}',
            storage=self.temp_db.name,
            log_level="INFO"
        )
        
        self.cmd_submit(args)
        
        # Verify storage was used
        mock_storage.assert_called_once_with(self.temp_db.name)
        mock_storage_instance.save_task.assert_called_once()
    
    @patch('pytaskq.cli.setup_logging')
    @patch('builtins.print')
    def test_submit_invalid_metadata(self, mock_print, mock_logging):
        """Test submitting a task with invalid metadata JSON."""
        args = argparse.Namespace(
            name="test_task",
            priority=None,
            metadata="invalid json",
            storage=None,
            log_level="INFO"
        )
        
        with self.assertRaises(SystemExit):
            self.cmd_submit(args)


class TestStatusCommand(unittest.TestCase):
    """Test the status command functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import cmd_status
        self.cmd_status = cmd_status
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.TaskQueue')
    @patch('builtins.print')
    def test_status_basic(self, mock_print, mock_task_queue, mock_logging):
        """Test basic status command."""
        mock_queue_instance = MagicMock()
        mock_queue_instance.size.return_value = 5
        mock_queue_instance.is_empty.return_value = False
        mock_queue_instance.peek.return_value = MagicMock(
            id="test-id",
            name="test-task",
            priority=3,
            status=MagicMock(value="pending")
        )
        mock_task_queue.return_value = mock_queue_instance
        
        args = argparse.Namespace(
            storage=None,
            log_level="INFO"
        )
        
        self.cmd_status(args)
        
        # Verify status was queried
        mock_queue_instance.size.assert_called_once()
        mock_queue_instance.is_empty.assert_called_once()
        mock_queue_instance.peek.assert_called_once()


class TestConfigCommand(unittest.TestCase):
    """Test the config command functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import cmd_config
        self.cmd_config = cmd_config
        self.temp_dir = tempfile.mkdtemp()
        self.temp_config = os.path.join(self.temp_dir, "test.yaml")
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def create_sample_config(self):
        """Create a sample config file."""
        config = {
            "max_workers": 8,
            "queue_size": 2000,
            "log_level": "DEBUG"
        }
        with open(self.temp_config, "w") as f:
            yaml.dump(config, f)
    
    @patch('builtins.print')
    def test_config_show_default(self, mock_print):
        """Test config show with default values."""
        args = argparse.Namespace(
            action="show",
            config=None,
            env=False,
            output=None
        )
        
        self.cmd_config(args)
        
        # Should have printed configuration
        self.assertTrue(mock_print.called)
    
    @patch('builtins.print')
    def test_config_show_from_file(self, mock_print):
        """Test config show from file."""
        self.create_sample_config()
        
        args = argparse.Namespace(
            action="show",
            config=self.temp_config,
            env=False,
            output=None
        )
        
        self.cmd_config(args)
        
        # Should have printed configuration
        self.assertTrue(mock_print.called)
    
    def test_config_validate_valid(self):
        """Test config validate with valid config."""
        self.create_sample_config()
        
        args = argparse.Namespace(
            action="validate",
            config=self.temp_config,
            env=False,
            output=None
        )
        
        # Should not raise an exception
        self.cmd_config(args)
    
    def test_config_generate(self):
        """Test config generate."""
        output_path = os.path.join(self.temp_dir, "generated.yaml")
        
        args = argparse.Namespace(
            action="generate",
            config=None,
            env=False,
            output=output_path
        )
        
        self.cmd_config(args)
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        
        # Verify content
        with open(output_path, "r") as f:
            config = yaml.safe_load(f)
        
        self.assertIsInstance(config, dict)
        self.assertIn("max_workers", config)


class TestMetricsCommand(unittest.TestCase):
    """Test the metrics command functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import cmd_metrics
        self.cmd_metrics = cmd_metrics
    
    @patch('pytaskq.cli.setup_logging')
    @patch('pytaskq.cli.MetricsCollector')
    @patch('builtins.print')
    def test_metrics_snapshot(self, mock_print, mock_metrics, mock_logging):
        """Test metrics command shows snapshot."""
        mock_collector = MagicMock()
        mock_snapshot = MagicMock(
            tasks_submitted=100,
            tasks_completed=80,
            tasks_failed=20,
            average_duration=1.5
        )
        mock_collector.snapshot.return_value = mock_snapshot
        mock_metrics.return_value = mock_collector
        
        args = argparse.Namespace(log_level="INFO")
        
        self.cmd_metrics(args)
        
        # Verify metrics collector was used
        mock_collector.snapshot.assert_called_once()
        self.assertTrue(mock_print.called)


class TestMainFunction(unittest.TestCase):
    """Test the main CLI entry point."""
    
    def setUp(self):
        """Set up test fixtures."""
        from pytaskq.cli import main
        self.main = main
    
    @patch('sys.argv', ['pytaskq', '--version'])
    @patch('pytaskq.cli.cmd_version')
    def test_main_version(self, mock_cmd_version):
        """Test main with version flag."""
        result = self.main()
        self.assertEqual(result, 0)
        mock_cmd_version.assert_called_once()
    
    @patch('sys.argv', ['pytaskq', 'status'])
    @patch('pytaskq.cli.cmd_status')
    def test_main_with_command(self, mock_cmd_status):
        """Test main with a valid command."""
        result = self.main()
        self.assertEqual(result, 0)
    
    @patch('sys.argv', ['pytaskq'])
    def test_main_no_command(self):
        """Test main with no command exits with error."""
        result = self.main()
        self.assertEqual(result, 1)
    
    @patch('sys.argv', ['pytaskq', 'status'])
    @patch('pytaskq.cli.cmd_status')
    def test_main_exception_handling(self, mock_cmd_status):
        """Test main handles exceptions gracefully."""
        mock_cmd_status.side_effect = Exception("Test error")
        
        result = self.main()
        self.assertEqual(result, 1)


class TestCLIIntegration(unittest.TestCase):
    """Integration tests for CLI functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_workflow(self):
        """Test complete end-to-end workflow."""
        from pytaskq.cli import create_parser
        
        # Test creating parser
        parser = create_parser()
        self.assertIsNotNone(parser)
        
        # Test parsing submit command
        args = parser.parse_args([
            "submit", "integration_test",
            "--priority", "2",
            "--storage", self.db_path
        ])
        self.assertEqual(args.name, "integration_test")
        self.assertEqual(args.priority, 2)
        
        # Test parsing status command
        args = parser.parse_args([
            "status",
            "--storage", self.db_path
        ])
        self.assertEqual(args.storage, self.db_path)
        
        # Test parsing config command
        args = parser.parse_args(["config", "generate"])
        self.assertEqual(args.action, "generate")


if __name__ == "__main__":
    unittest.main()