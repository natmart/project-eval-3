"""
Unit tests for the CLI interface.
"""

import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pytaskq.cli import (
    CLIError,
    create_parser,
    handle_config,
    handle_list,
    handle_metrics,
    handle_status,
    handle_submit,
    handle_worker_start,
    load_config,
    main,
)


class TestArgumentParser:
    """Test argument parser creation and parsing."""

    def test_create_parser_returns_parser(self):
        """Test that create_parser returns an ArgumentParser instance."""
        parser = create_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_version_flag(self):
        """Test version flag parsing."""
        parser = create_parser()
        args = parser.parse_args(["--version"])
        assert args.version is True

    def test_config_flag(self):
        """Test config file flag parsing."""
        parser = create_parser()
        args = parser.parse_args(["--config", "test.yaml"])
        assert args.config == "test.yaml"

    def test_verbose_flag(self):
        """Test verbose flag parsing."""
        parser = create_parser()
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True

    def test_quiet_flag(self):
        """Test quiet flag parsing."""
        parser = create_parser()
        args = parser.parse_args(["--quiet"])
        assert args.quiet is True

    def test_worker_start_command(self):
        """Test worker start command parsing."""
        parser = create_parser()
        args = parser.parse_args(["worker", "start", "--workers", "4"])
        assert args.command == "worker"
        assert args.worker_action == "start"
        assert args.workers == 4

    def test_worker_start_with_all_options(self):
        """Test worker start command with all options."""
        parser = create_parser()
        args = parser.parse_args(
            [
                "worker",
                "start",
                "--workers",
                "2",
                "--poll-interval",
                "0.5",
                "--daemon",
                "--queue-size",
                "500",
            ]
        )
        assert args.workers == 2
        assert args.poll_interval == 0.5
        assert args.daemon is True
        assert args.queue_size == 500

    def test_submit_command(self):
        """Test submit command parsing."""
        parser = create_parser()
        args = parser.parse_args(["submit", "test任务"])
        assert args.command == "submit"
        assert args.task_name == "test任务"

    def test_submit_with_options(self):
        """Test submit command with all options."""
        parser = create_parser()
        payload = '{"key": "value"}'
        metadata = '{"meta": "data"}'
        args = parser.parse_args(
            [
                "submit",
                "test_task",
                "--payload",
                payload,
                "--priority",
                "1",
                "--max-retries",
                "5",
                "--metadata",
                metadata,
            ]
        )
        assert args.task_name == "test_task"
        assert args.payload == payload
        assert args.priority == 1
        assert args.max_retries == 5
        assert args.metadata == metadata

    def test_status_command(self):
        """Test status command parsing."""
        parser = create_parser()
        args = parser.parse_args(["status"])
        assert args.command == "status"
        assert args.json is False

    def test_status_with_json(self):
        """Test status command with JSON flag."""
        parser = create_parser()
        args = parser.parse_args(["status", "--json"])
        assert args.command == "status"
        assert args.json is True

    def test_list_command(self):
        """Test list command parsing."""
        parser = create_parser()
        args = parser.parse_args(["list"])
        assert args.command == "list"
        assert args.limit == 100

    def test_list_with_options(self):
        """Test list command with all options."""
        parser = create_parser()
        args = parser.parse_args(
            ["list", "--status", "pending", "--limit", "50", "--json"]
        )
        assert args.status == "pending"
        assert args.limit == 50
        assert args.json is True

    def test_metrics_command(self):
        """Test metrics command parsing."""
        parser = create_parser()
        args = parser.parse_args(["metrics"])
        assert args.command == "metrics"
        assert args.reset is False

    def test_metrics_with_reset(self):
        """Test metrics command with reset flag."""
        parser = create_parser()
        args = parser.parse_args(["metrics", "--reset"])
        assert args.command == "metrics"
        assert args.reset is True

    def test_config_command(self):
        """Test config command parsing."""
        parser = create_parser()
        args = parser.parse_args(["config", "--show"])
        assert args.command == "config"
        assert args.show is True

    def test_config_validate(self):
        """Test config validate action."""
        parser = create_parser()
        args = parser.parse_args(["config", "--validate"])
        assert args.validate is True

    def test_config_generate(self):
        """Test config generate action."""
        parser = create_parser()
        args = parser.parse_args(["config", "--generate", "config.yaml"])
        assert args.generate == "config.yaml"


class TestLoadConfig:
    """Test configuration loading."""

    def test_load_config_without_file(self):
        """Test loading config without a file uses defaults."""
        config = load_config(None)
        assert config is not None
        assert config.get("max_workers") == 4

    def test_load_config_with_empty_string(self):
        """Test loading config with empty string uses defaults."""
        config = load_config("")
        assert config is not None

    def test_load_config_nonexistent_file(self):
        """Test loading config from non-existent file raises error."""
        with pytest.raises(CLIError):
            load_config("/nonexistent/path/to/config.yaml")

    @patch("pytaskq.cli.Config.from_file_with_env_override")
    def test_load_config_delegate_to_config_class(self, mock_from_file):
        """Test that load_config delegates to Config.from_file_with_env_override."""
        mock_config = MagicMock()
        mock_from_file.return_value = mock_config

        result = load_config("test.yaml")
        mock_from_file.assert_called_once_with("test.yaml")
        assert result == mock_config


class TestHandleWorkerStart:
    """Test worker start handler."""

    @patch("pytaskq.cli.WorkerPool")
    @patch("pytaskq.cli.PriorityQueue")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_worker_start_basic(
        self, mock_logger, mock_load_config, mock_queue_class, mock_pool_class
    ):
        """Test basic worker start handling."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        args = argparse.Namespace(
            workers=2,
            poll_interval=0.5,
            daemon=False,
            queue_size=None,
            config=None,
        )

        with patch("time.sleep", side_effect=KeyboardInterrupt):
            result = handle_worker_start(args)

        mock_pool.start_all.assert_called_once()
        mock_pool.stop_all.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.WorkerPool")
    @patch("pytaskq.cli.PriorityQueue")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_worker_start_with_daemon(
        self, mock_logger, mock_load_config, mock_queue_class, mock_pool_class
    ):
        """Test worker start with daemon mode."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        args = argparse.Namespace(
            workers=3,
            poll_interval=0.2,
            daemon=True,
            queue_size=500,
            config=None,
        )

        result = handle_worker_start(args)

        mock_pool.start_all.assert_called_once()
        mock_pool.stop_all.assert_not_called()
        assert result == 0

    @patch("pytaskq.cli.WorkerPool")
    @patch("pytaskq.cli.PriorityQueue")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_worker_start_with_config_file(
        self, mock_logger, mock_load_config, mock_queue_class, mock_pool_class
    ):
        """Test worker start with config file."""
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda k, d=None: {"queue_size": 1000}.get(k, d)
        mock_load_config.return_value = mock_config
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        args = argparse.Namespace(
            workers=None,
            poll_interval=0.1,
            daemon=False,
            queue_size=None,
            config="test.yaml",
        )

        with patch("time.sleep", side_effect=KeyboardInterrupt):
            result = handle_worker_start(args)

        mock_load_config.assert_called_once_with("test.yaml")
        assert result == 0

    @patch("pytaskq.cli.WorkerPool")
    @patch("pytaskq.cli.PriorityQueue")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_worker_start_error_handling(
        self, mock_logger, mock_load_config, mock_queue_class, mock_pool_class
    ):
        """Test error handling in worker start."""
        mock_load_config.side_effect = Exception("Test error")

        args = argparse.Namespace(
            workers=1,
            poll_interval=0.1,
            daemon=False,
            queue_size=None,
            config=None,
        )

        # Exception is caught by try/except, but we test the path
        result = handle_worker_start(args)
        assert result == 1


class TestHandleSubmit:
    """Test task submission handler."""

    @patch("pytaskq.cli.Task")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.logger")
    def test_handle_submit_basic(
        self, mock_logger, mock_storage_class, mock_load_config, mock_task_class
    ):
        """Test basic task submission."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_task = MagicMock(id="task-123", name="test", priority=0)
        mock_task.status.value = "pending"
        mock_task_class.return_value = mock_task

        args = argparse.Namespace(
            task_name="test_task",
            payload=None,
            priority=0,
            max_retries=3,
            metadata=None,
            config=None,
        )

        result = handle_submit(args)

        mock_task_class.assert_called_once()
        mock_storage.save_task.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.Task")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.logger")
    def test_handle_submit_with_payload(
        self, mock_logger, mock_storage_class, mock_load_config, mock_task_class
    ):
        """Test task submission with JSON payload."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_task = MagicMock(id="task-123", name="test", priority=0)
        mock_task.status.value = "pending"
        mock_task_class.return_value = mock_task

        payload = '{"key": "value"}'
        args = argparse.Namespace(
            task_name="test_task",
            payload=payload,
            priority=1,
            max_retries=5,
            metadata=None,
            config=None,
        )

        result = handle_submit(args)

        # Verify payload was parsed and passed to Task
        call_kwargs = mock_task_class.call_args[1]
        assert call_kwargs["payload"] == {"key": "value"}
        assert result == 0

    @patch("pytaskq.cli.Task")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.logger")
    def test_handle_submit_invalid_json_payload(
        self, mock_logger, mock_storage_class, mock_load_config, mock_task_class
    ):
        """Test task submission with invalid JSON payload."""
        payload = '{invalid json}'

        args = argparse.Namespace(
            task_name="test_task",
            payload=payload,
            priority=0,
            max_retries=3,
            metadata=None,
            config=None,
        )

        result = handle_submit(args)

        mock_logger.error.assert_called()
        assert result == 1

    @patch("pytaskq.cli.Task")
    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.logger")
    def test_handle_submit_with_metadata(
        self, mock_logger, mock_storage_class, mock_load_config, mock_task_class
    ):
        """Test task submission with metadata."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_task = MagicMock(id="task-123", name="test", priority=0)
        mock_task.status.value = "pending"
        mock_task_class.return_value = mock_task

        metadata = '{"meta": "data"}'
        args = argparse.Namespace(
            task_name="test_task",
            payload=None,
            priority=0,
            max_retries=3,
            metadata=metadata,
            config=None,
        )

        result = handle_submit(args)

        assert hasattr(mock_task, "metadata")
        assert result == 0

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.Task")
    @patch("pytaskq.cli.logger")
    def test_handle_submit_exception(
        self, mock_logger, mock_task_class, mock_storage_class, mock_load_config
    ):
        """Test exception handling in submit."""
        mock_task_class.side_effect = Exception("Task creation failed")

        args = argparse.Namespace(
            task_name="test_task",
            payload=None,
            priority=0,
            max_retries=3,
            metadata=None,
            config=None,
        )

        result = handle_submit(args)

        mock_logger.error.assert_called()
        assert result == 1


class TestHandleStatus:
    """Test status handler."""

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_status_basic(self, mock_print, mock_storage_class, mock_load_config):
        """Test basic status display."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        mock_storage.list_tasks.return_value = []
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(config=None, json=False)
        result = handle_status(args)

        assert result == 0
        mock_print.assert_called()

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_status_json(self, mock_print, mock_storage_class, mock_load_config):
        """Test status display in JSON format."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        mock_storage.list_tasks.return_value = []
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(config=None, json=True)
        result = handle_status(args)

        assert result == 0
        # Verify JSON output
        print_calls = [str(call) for call in mock_print.call_args_list]
        assert any("total_tasks" in call for call in print_calls)

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_status_with_tasks(
        self, mock_print, mock_storage_class, mock_load_config
    ):
        """Test status with tasks in different states."""
        from pytaskq import Task, TaskStatus

        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_storage = MagicMock()
        tasks = [
            Task(name="task1", status=TaskStatus.PENDING),
            Task(name="task2", status=TaskStatus.RUNNING),
            Task(name="task3", status=TaskStatus.COMPLETED),
            Task(name="task4", status=TaskStatus.FAILED),
        ]
        mock_storage.list_tasks.return_value = tasks
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(config=None, json=False)
        result = handle_status(args)

        assert result == 0

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.SQLiteBackend")
    @patch("pytaskq.cli.logger")
    def test_handle_status_exception(
        self, mock_logger, mock_storage_class, mock_load_config
    ):
        """Test exception handling in status."""
        mock_storage_class.side_effect = Exception("Storage error")

        args = argparse.Namespace(config=None, json=False)
        result = handle_status(args)

        mock_logger.error.assert_called()
        assert result == 1


class TestHandleList:
    """Test list handler."""

    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_list_basic(self, mock_print, mock_storage_class):
        """Test basic task listing."""
        from pytaskq import Task, TaskStatus

        mock_storage = MagicMock()
        task = Task(name="test_task", status=TaskStatus.PENDING)
        mock_storage.list_tasks.return_value = [task]
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(status=None, limit=100, json=False)
        result = handle_list(args)

        assert result == 0
        mock_print.assert_called()

    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_list_with_status_filter(
        self, mock_print, mock_storage_class
    ):
        """Test task listing with status filter."""
        from pytaskq import Task, TaskStatus

        mock_storage = MagicMock()
        tasks = [
            Task(name="task1", status=TaskStatus.PENDING),
            Task(name="task2", status=TaskStatus.RUNNING),
            Task(name="task3", status=TaskStatus.COMPLETED),
        ]
        mock_storage.list_tasks.return_value = tasks
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(status="pending", limit=100, json=False)
        result = handle_list(args)

        assert result == 0

    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_list_with_limit(self, mock_print, mock_storage_class):
        """Test task listing with limit."""
        from pytaskq import Task, TaskStatus

        mock_storage = MagicMock()
        tasks = [Task(name=f"task{i}", status=TaskStatus.PENDING) for i in range(50)]
        mock_storage.list_tasks.return_value = tasks
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(status=None, limit=10, json=False)
        result = handle_list(args)

        assert result == 0

    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    def test_handle_list_json(self, mock_print, mock_storage_class):
        """Test task listing in JSON format."""
        from pytaskq import Task, TaskStatus

        mock_storage = MagicMock()
        task = Task(name="test_task", status=TaskStatus.PENDING)
        mock_storage.list_tasks.return_value = [task]
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(status=None, limit=100, json=True)
        result = handle_list(args)

        assert result == 0

    @patch("pytaskq.cli.SQLiteBackend")
    @patch("builtins.print")
    @patch("pytaskq.cli.logger")
    def test_handle_list_invalid_status(
        self, mock_logger, mock_print, mock_storage_class
    ):
        """Test task listing with invalid status."""
        mock_storage = MagicMock()
        mock_storage.list_tasks.return_value = []
        mock_storage_class.return_value = mock_storage

        args = argparse.Namespace(status="invalid_status", limit=100, json=False)
        result = handle_list(args)

        mock_logger.error.assert_called()
        assert result == 1


class TestHandleMetrics:
    """Test metrics handler."""

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.MetricsCollector")
    @patch("builtins.print")
    def test_handle_metrics_basic(self, mock_print, mock_metrics_class, mock_load_config):
        """Test basic metrics display."""
        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_metrics = MagicMock()
        mock_snapshot = MagicMock(tasks_submitted=0, tasks_completed=0, tasks_failed=0)
        mock_metrics.snapshot.return_value = mock_snapshot
        mock_metrics_class.return_value = mock_metrics

        args = argparse.Namespace(config=None, reset=False, json=False)
        result = handle_metrics(args)

        assert result == 0
        mock_print.assert_called()

    @patch("pytaskq.cli.MetricsCollector")
    @patch("pytaskq.cli.logger")
    def test_handle_metrics_reset(self, mock_logger, mock_metrics_class):
        """Test metrics reset."""
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics

        args = argparse.Namespace(config=None, reset=True, json=False)
        result = handle_metrics(args)

        mock_metrics.reset.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.MetricsCollector")
    @patch("builtins.print")
    def test_handle_metrics_json(self, mock_print, mock_metrics_class, mock_load_config):
        """Test metrics display in JSON format."""
        from datetime import datetime

        mock_load_config.return_value = MagicMock(get=lambda k, d=None: d)
        mock_metrics = MagicMock()
        mock_snapshot = MagicMock(
            tasks_submitted=10,
            tasks_completed=8,
            tasks_failed=2,
            average_duration=1.5,
            timestamp=datetime.utcnow(),
        )
        mock_metrics.snapshot.return_value = mock_snapshot
        mock_metrics_class.return_value = mock_metrics

        args = argparse.Namespace(config=None, reset=False, json=True)
        result = handle_metrics(args)

        assert result == 0
        print_calls = [str(call) for call in mock_print.call_args_list]
        assert any("tasks_submitted" in call for call in print_calls)


class TestHandleConfig:
    """Test config handler."""

    @patch("builtins.open", create=True)
    @patch("pytaskq.cli.logger")
    def test_handle_config_generate(self, mock_logger, mock_open):
        """Test config file generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "test_config.yaml")
            mock_open.return_value.__enter__ = MagicMock()
            mock_open.return_value.__exit__ = MagicMock()
            mock_open.return_value.write = MagicMock()

            args = argparse.Namespace(
                generate=config_path, show=False, validate=False, config=None
            )

            result = handle_config(args)

            mock_logger.info.assert_called()
            assert result == 0

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_config_validate_valid(self, mock_logger, mock_load_config):
        """Test config validation with valid config."""
        mock_config = MagicMock()
        mock_config.validate.return_value = True
        mock_load_config.return_value = mock_config

        args = argparse.Namespace(
            generate=None, show=False, validate=True, config=None
        )

        result = handle_config(args)

        mock_config.validate.assert_called_once()
        mock_logger.info.assert_called()
        assert result == 0

    @patch("pytaskq.cli.load_config")
    @patch("pytaskq.cli.logger")
    def test_handle_config_validate_invalid(self, mock_logger, mock_load_config):
        """Test config validation with invalid config."""
        mock_config = MagicMock()
        mock_config.validate.side_effect = ValueError("Invalid config")
        mock_load_config.return_value = mock_config

        args = argparse.Namespace(
            generate=None, show=False, validate=True, config=None
        )

        result = handle_config(args)

        mock_config.validate.assert_called_once()
        mock_logger.error.assert_called()
        assert result == 1

    @patch("pytaskq.cli.load_config")
    @patch("builtins.print")
    def test_handle_config_show(self, mock_print, mock_load_config):
        """Test config show."""
        mock_config = MagicMock()
        mock_config.to_dict.return_value = {"max_workers": 4, "queue_size": 1000}
        mock_load_config.return_value = mock_config

        args = argparse.Namespace(
            generate=None, show=True, validate=False, config=None
        )

        result = handle_config(args)

        mock_print.assert_called()
        assert result == 0


class TestMainFunction:
    """Test main CLI entry point."""

    @patch("pytaskq.cli.create_parser")
    def test_main_version(self, mock_parser_class):
        """Test main with version flag."""
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = MagicMock(
            version=True, command=None, verbose=False, quiet=False
        )
        mock_parser_class.return_value = mock_parser

        with patch("builtins.print") as mock_print:
            result = main(["--version"])

        mock_print.assert_called()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_worker_start")
    def test_main_worker_start(self, mock_handle_worker, mock_parser_class):
        """Test main with worker start command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="worker",
            verbose=False,
            quiet=False,
            worker_action="start",
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_worker.return_value = 0

        result = main(["worker", "start"])

        mock_handle_worker.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_submit")
    def test_main_submit(self, mock_handle_submit, mock_parser_class):
        """Test main with submit command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="submit",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_submit.return_value = 0

        result = main(["submit", "test_task"])

        mock_handle_submit.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_status")
    def test_main_status(self, mock_handle_status, mock_parser_class):
        """Test main with status command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="status",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_status.return_value = 0

        result = main(["status"])

        mock_handle_status.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_list")
    def test_main_list(self, mock_handle_list, mock_parser_class):
        """Test main with list command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="list",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_list.return_value = 0

        result = main(["list"])

        mock_handle_list.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_metrics")
    def test_main_metrics(self, mock_handle_metrics, mock_parser_class):
        """Test main with metrics command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="metrics",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_metrics.return_value = 0

        result = main(["metrics"])

        mock_handle_metrics.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.handle_config")
    def test_main_config(self, mock_handle_config, mock_parser_class):
        """Test main with config command."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="config",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser
        mock_handle_config.return_value = 0

        result = main(["config", "--show"])

        mock_handle_config.assert_called_once()
        assert result == 0

    @patch("pytaskq.cli.create_parser")
    def test_main_no_command(self, mock_parser_class):
        """Test main with no command specified."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False, command=None, verbose=False, quiet=False, config=None
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser

        result = main([])

        mock_parser.print_help.assert_called_once()
        assert result == 1

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.logger")
    def test_main_keyboard_interrupt(self, mock_logger, mock_parser_class):
        """Test main with keyboard interrupt."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="status",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser

        with patch("pytaskq.cli.handle_status", side_effect=KeyboardInterrupt):
            result = main(["status"])

        mock_logger.info.assert_called()
        assert result == 130

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.logger")
    def test_main_cli_error(self, mock_logger, mock_parser_class):
        """Test main with CLIError."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="status",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser

        with patch("pytaskq.cli.handle_status", side_effect=CLIError("Test error")):
            result = main(["status"])

        mock_logger.error.assert_called()
        assert result == 1

    @patch("pytaskq.cli.create_parser")
    @patch("pytaskq.cli.logger")
    def test_main_unexpected_error(self, mock_logger, mock_parser_class):
        """Test main with unexpected error."""
        mock_parser = MagicMock()
        args = MagicMock(
            version=False,
            command="status",
            verbose=False,
            quiet=False,
            config=None,
        )
        mock_parser.parse_args.return_value = args
        mock_parser_class.return_value = mock_parser

        with patch("pytaskq.cli.handle_status", side_effect=Exception("Unexpected")):
            result = main(["status"])

        mock_logger.error.assert_called()
        assert result == 1


class TestCLIError:
    """Test CLIError exception class."""

    def test_cli_error_instantiation(self):
        """Test CLIError can be instantiated."""
        error = CLIError("Test error message")
        assert str(error) == "Test error message"
        assert isinstance(error, Exception)

    def test_cli_error_can_be_raised(self):
        """Test CLIError can be raised and caught."""
        with pytest.raises(CLIError) as exc_info:
            raise CLIError("Test error")

        assert str(exc_info.value) == "Test error"