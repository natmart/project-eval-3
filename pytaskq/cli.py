"""
CLI interface for PyTaskQ.

This module provides command-line interface for managing task queues,
workers, tasks, and metrics.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from typing import Optional

from pytaskq import (
    Config,
    MetricsCollector,
    PriorityQueue,
    Task,
    TaskStatus,
    Worker,
    WorkerPool,
)
from pytaskq.storage import SQLiteBackend

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class CLIError(Exception):
    """Base exception for CLI errors."""

    pass


def create_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser with all subcommands.

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog="pytaskq",
        description="PyTaskQ - A Python Task Queue Library v2",
    )
    parser.add_argument(
        "--version", action="store_true", help="Show version and exit"
    )
    parser.add_argument(
        "--config", "-c", type=str, help="Path to configuration file"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Suppress all output except errors"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Worker command
    worker_parser = subparsers.add_parser("worker", help="Manage workers")
    worker_subparsers = worker_parser.add_subparsers(dest="worker_action")

    worker_start = worker_subparsers.add_parser("start", help="Start worker(s)")
    worker_start.add_argument(
        "--workers", "-w", type=int, default=1, help="Number of workers to start"
    )
    worker_start.add_argument(
        "--poll-interval",
        "-p",
        type=float,
        default=0.1,
        help="Poll interval in seconds",
    )
    worker_start.add_argument(
        "--daemon", "-d", action="store_true", help="Run worker as daemon"
    )
    worker_start.add_argument(
        "--queue-size",
        type=int,
        help="Maximum queue size",
    )

    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a task to the queue")
    submit_parser.add_argument("task_name", type=str, help="Name of the task")
    submit_parser.add_argument(
        "--payload",
        "-p",
        type=str,
        help="Task payload (JSON string)",
    )
    submit_parser.add_argument(
        "--priority",
        "-r",
        type=int,
        default=0,
        help="Task priority (lower = higher priority)",
    )
    submit_parser.add_argument(
        "--max-retries",
        "-m",
        type=int,
        default=3,
        help="Maximum retry attempts",
    )
    submit_parser.add_argument(
        "--metadata",
        "-md",
        type=str,
        help="Task metadata (JSON string)",
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Show queue status")
    status_parser.add_argument(
        "--json", action="store_true", help="Output in JSON format"
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List tasks")
    list_parser.add_argument(
        "--status", "-s", type=str, help="Filter by task status"
    )
    list_parser.add_argument(
        "--limit", "-l", type=int, default=100, help="Maximum number of tasks to show"
    )
    list_parser.add_argument(
        "--json", action="store_true", help="Output in JSON format"
    )

    # Metrics command
    metrics_parser = subparsers.add_parser("metrics", help="Show metrics")
    metrics_parser.add_argument(
        "--reset", action="store_true", help="Reset metrics"
    )
    metrics_parser.add_argument(
        "--json", action="store_true", help="Output in JSON format"
    )

    # Config command
    config_parser = subparsers.add_parser("config", help="Manage configuration")
    config_parser.add_argument(
        "--validate", action="store_true", help="Validate configuration"
    )
    config_parser.add_argument(
        "--show", action="store_true", help="Show current configuration"
    )
    config_parser.add_argument(
        "--generate", "-g", type=str, help="Generate default config to file"
    )

    return parser


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from file or use defaults.

    Args:
        config_path: Optional path to configuration file

    Returns:
        Config instance
    """
    if config_path:
        try:
            return Config.from_file_with_env_override(config_path)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise CLIError(f"Configuration file not found: {config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise CLIError(f"Error loading configuration: {e}")
    else:
        return Config.from_env()


def handle_worker_start(args: argparse.Namespace) -> int:
    """
    Handle the 'worker start' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    config = load_config(args.config) if hasattr(args, "config") else Config()
    queue_size = args.queue_size or config.get("queue_size", 1000)
    worker_count = args.workers or config.get("max_workers", 1)
    poll_interval = args.poll_interval

    logger.info(f"Starting {worker_count} worker(s)...")
    logger.info(f"Queue size: {queue_size}")
    logger.info(f"Poll interval: {poll_interval}s")

    queue = PriorityQueue()
    pool = WorkerPool(
        queue=queue,
        worker_count=worker_count,
        poll_interval=poll_interval,
    )

    if args.daemon:
        # Run as daemon - exit after starting
        pool.start_all()
        logger.info(f"Worker pool started with {worker_count} worker(s)")
        return 0
    else:
        # Run and wait for signal
        pool.start_all()
        logger.info(f"Worker pool running. Press Ctrl+C to stop.")

        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\nShutting down workers...")
            pool.stop_all()
            logger.info("Workers stopped.")
            return 0


def handle_submit(args: argparse.Namespace) -> int:
    """
    Handle the 'submit' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Parse payload if provided
        payload = None
        if args.payload:
            try:
                payload = json.loads(args.payload)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON payload: {e}")
                return 1

        # Create task
        task = Task(
            name=args.task_name,
            payload=payload,
            priority=args.priority,
            max_retries=args.max_retries,
        )

        # Add metadata if provided
        if args.metadata:
            try:
                metadata = json.loads(args.metadata)
                if isinstance(metadata, dict):
                    task.metadata = metadata
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON metadata: {e}")
                return 1

        # Load queue from storage or create new
        config = load_config(args.config) if hasattr(args, "config") else Config()
        storage = SQLiteBackend(":memory:")

        # Submit task
        storage.save_task(task)
        logger.info(f"Task submitted successfully!")
        logger.info(f"  Task ID: {task.id}")
        logger.info(f"  Name: {task.name}")
        logger.info(f"  Priority: {task.priority}")
        logger.info(f"  Status: {task.status.value}")

        return 0

    except Exception as e:
        logger.error(f"Error submitting task: {e}")
        return 1


def handle_status(args: argparse.Namespace) -> int:
    """
    Handle the 'status' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        config = load_config(args.config) if hasattr(args, "config") else Config()
        storage = SQLiteBackend(":memory:")

        # Get statistics
        all_tasks = storage.list_tasks()
        pending = [t for t in all_tasks if t.status == TaskStatus.PENDING]
        running = [t for t in all_tasks if t.status == TaskStatus.RUNNING]
        completed = [t for t in all_tasks if t.status == TaskStatus.COMPLETED]
        failed = [t for t in all_tasks if t.status == TaskStatus.FAILED]

        status_info = {
            "total_tasks": len(all_tasks),
            "pending": len(pending),
            "running": len(running),
            "completed": len(completed),
            "failed": len(failed),
            "queue_size_threshold": config.get("queue_size", 1000),
        }

        if args.json:
            print(json.dumps(status_info, indent=2))
        else:
            print("=== Queue Status ===")
            print(f"Total Tasks: {status_info['total_tasks']}")
            print(f"  Pending:   {status_info['pending']}")
            print(f"  Running:   {status_info['running']}")
            print(f"  Completed: {status_info['completed']}")
            print(f"  Failed:    {status_info['failed']}")
            print(f"Queue Size Threshold: {status_info['queue_size_threshold']}")

        return 0

    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return 1


def handle_list(args: argparse.Namespace) -> int:
    """
    Handle the 'list' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        storage = SQLiteBackend(":memory:")
        tasks = storage.list_tasks()

        # Filter by status if requested
        if args.status:
            try:
                filter_status = TaskStatus(args.status.lower())
                tasks = [t for t in tasks if t.status == filter_status]
            except ValueError:
                logger.error(f"Invalid status: {args.status}")
                logger.error(f"Valid statuses: {', '.join([s.value for s in TaskStatus])}")
                return 1

        # Limit results
        tasks = tasks[: args.limit]

        if args.json:
            tasks_data = [task.to_dict() for task in tasks]
            print(json.dumps(tasks_data, indent=2))
        else:
            print(f"=== Tasks ({len(tasks)} shown) ===")
            for task in tasks:
                print(f"ID: {task.id}")
                print(f"  Name: {task.name}")
                print(f"  Status: {task.status.value}")
                print(f"  Priority: {task.priority}")
                print(f"  Created: {task.created_at.isoformat()}")
                print(f"  Retries: {task.retry_count}/{task.max_retries}")
                print()

        return 0

    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        return 1


def handle_metrics(args: argparse.Namespace) -> int:
    """
    Handle the 'metrics' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        config = load_config(args.config) if hasattr(args, "config") else Config()
        # For demonstration, create a new metrics collector
        # In practice, this would retrieve from running workers
        metrics = MetricsCollector()

        if args.reset:
            metrics.reset()
            logger.info("Metrics reset successfully")
            return 0

        # Get snapshot
        snapshot = metrics.snapshot()
        metrics_info = {
            "tasks_submitted": snapshot.tasks_submitted,
            "tasks_completed": snapshot.tasks_completed,
            "tasks_failed": snapshot.tasks_failed,
            "average_duration": snapshot.average_duration,
            "timestamp": snapshot.timestamp.isoformat(),
        }

        if args.json:
            print(json.dumps(metrics_info, indent=2))
        else:
            print("=== Metrics ===")
            print(f"Tasks Submitted: {metrics_info['tasks_submitted']}")
            print(f"Tasks Completed: {metrics_info['tasks_completed']}")
            print(f"Tasks Failed: {metrics_info['tasks_failed']}")
            print(f"Average Duration: {metrics_info['average_duration']:.2f}s")
            print(f"Timestamp: {metrics_info['timestamp']}")

        return 0

    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return 1


def handle_config(args: argparse.Namespace) -> int:
    """
    Handle the 'config' command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        if args.generate:
            # Generate default config file
            config = Config()
            with open(args.generate, "w") as f:
                f.write("max_workers: 4\n")
                f.write("queue_size: 1000\n")
                f.write("worker_timeout: 300\n")
                f.write("task_timeout: 60\n")
                f.write("retry_attempts: 3\n")
                f.write("log_level: INFO\n")
                f.write("heartbeat_interval: 60\n")
            logger.info(f"Default configuration written to: {args.generate}")
            return 0

        config = load_config(args.config) if hasattr(args, "config") else Config()

        if args.validate:
            try:
                config.validate()
                logger.info("Configuration is valid")
                return 0
            except ValueError as e:
                logger.error(f"Configuration validation failed: {e}")
                return 1

        if args.show:
            config_dict = config.to_dict()
            print(json.dumps(config_dict, indent=2))
            return 0

        # If no action specified, show config
        config_dict = config.to_dict()
        print(json.dumps(config_dict, indent=2))
        return 0

    except Exception as e:
        logger.error(f"Error handling config: {e}")
        return 1


def main(argv: Optional[list] = None) -> int:
    """
    Main entry point for the CLI.

    Args:
        argv: Optional list of command-line arguments (default: sys.argv[1:])

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Handle version flag
    if args.version:
        from pytaskq import __version__

        print(f"PyTaskQ version {__version__}")
        return 0

    # Set logging level based on flags
    if args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If no command specified, show help
    if not args.command:
        parser.print_help()
        return 1

    # Route to appropriate handler
    try:
        if args.command == "worker":
            if args.worker_action == "start":
                return handle_worker_start(args)
            else:
                parser.print_help()
                return 1

        elif args.command == "submit":
            return handle_submit(args)

        elif args.command == "status":
            return handle_status(args)

        elif args.command == "list":
            return handle_list(args)

        elif args.command == "metrics":
            return handle_metrics(args)

        elif args.command == "config":
            return handle_config(args)

        else:
            parser.print_help()
            return 1

    except CLIError as e:
        logger.error(str(e))
        return 1
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    sys.exit(main())