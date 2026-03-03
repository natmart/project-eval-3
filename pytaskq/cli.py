#!/usr/bin/env python3
"""
CLI interface for PyTaskQ.

This module provides a command-line interface for interacting with the task queue,
including starting workers, submitting tasks, managing configuration, and viewing metrics.
"""

import argparse
import sys
import json
import time
import signal
import logging
from typing import Optional, Any, Dict, List
from pathlib import Path


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration for the CLI.

    Args:
        log_level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_worker(args: argparse.Namespace) -> None:
    """
    Start a worker process.

    Args:
        args: Parsed command-line arguments.
    """
    setup_logging(args.log_level)
    
    # Import here to avoid issues
    from pytaskq import TaskQueue, WorkerPool, Scheduler, Config, SQLiteBackend
    
    # Load configuration
    if args.config:
        config = Config.from_file(args.config)
    else:
        config = Config.from_env()
    
    config.validate()
    logging.info(f"Configuration loaded: {config}")
    
    # Create storage backend if specified
    storage = None
    if args.storage:
        storage = SQLiteBackend(args.storage)
        logging.info(f"Using storage backend: {args.storage}")
    
    # Create task queue and scheduler
    queue = TaskQueue()
    scheduler = Scheduler(queue, storage=storage, config=config)
    
    # Create worker pool
    num_workers = args.workers or config.get("max_workers", 4)
    worker_pool = WorkerPool(num_workers=num_workers, queue=queue)
    
    logging.info(f"Starting {num_workers} worker(s)")
    
    # Setup graceful shutdown
    shutdown_requested = False
    
    def signal_handler(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        worker_pool.shutdown()
        scheduler.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the scheduler and worker pool
    scheduler.start()
    worker_pool.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
            # Check for shutdown
            if shutdown_requested:
                break
                
            # Optionally print metrics
            if args.metrics_interval:
                metrics = worker_pool.get_metrics()
                logging.info(f"Worker metrics: {metrics}")
                
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, shutting down...")
        worker_pool.shutdown()
        scheduler.stop()
    finally:
        if storage:
            storage.close()
        logging.info("Worker process stopped")


def cmd_submit(args: argparse.Namespace) -> None:
    """
    Submit a task to the queue.

    Args:
        args: Parsed command-line arguments.
    """
    setup_logging(args.log_level)
    
    # Import here to avoid issues
    from pytaskq import TaskQueue, Task, SQLiteBackend
    
    # Create storage backend if specified
    storage = None
    if args.storage:
        storage = SQLiteBackend(args.storage)
    
    # Create task queue
    queue = TaskQueue()
    
    # Parse task metadata
    task_name = args.name
    task_priority = args.priority if args.priority is not None else 5
    
    # Create the task
    task = Task(name=task_name, priority=task_priority)
    
    # Add any metadata
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
            for key, value in metadata.items():
                task.add_metadata(key, value)
        except json.JSONDecodeError as e:
            print(f"Error parsing metadata JSON: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Enqueue the task
    queue.enqueue(task)
    
    # Persist to storage if specified
    if storage:
        storage.save_task(task)
    
    print(f"Task submitted successfully:")
    print(f"  ID: {task.id}")
    print(f"  Name: {task.name}")
    print(f"  Priority: {task.priority}")
    print(f"  Status: {task.status.value}")
    
    if storage:
        storage.close()


def cmd_status(args: argparse.Namespace) -> None:
    """
    Show the status of the task queue.

    Args:
        args: Parsed command-line arguments.
    """
    setup_logging(args.log_level)
    
    # Import here to avoid issues
    from pytaskq import TaskQueue, SQLiteBackend
    
    # Create storage backend if specified
    storage = None
    if args.storage:
        storage = SQLiteBackend(args.storage)
    
    # Create task queue
    queue = TaskQueue()
    
    print("Queue Status")
    print("=" * 50)
    print(f"Queue size: {queue.size()}")
    print(f"Is empty: {queue.is_empty()}")
    
    # Show next task if available
    next_task = queue.peek()
    if next_task:
        print(f"\nNext task:")
        print(f"  ID: {next_task.id}")
        print(f"  Name: {next_task.name}")
        print(f"  Priority: {next_task.priority}")
        print(f"  Status: {next_task.status.value}")
    else:
        print("\nNo tasks in queue")
    
    # Show storage statistics if available
    if storage:
        tasks = storage.get_all_tasks()
        status_counts: Dict[str, int] = {}
        
        for task in tasks:
            status = task.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\nStorage Statistics (Total: {len(tasks)} tasks)")
        print("-" * 50)
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count}")
        
        storage.close()


def cmd_config(args: argparse.Namespace) -> None:
    """
    Manage configuration.

    Args:
        args: Parsed command-line arguments.
    """
    # Import here to avoid issues
    from pytaskq import Config
    import yaml
    
    if args.action == "show":
        # Show current configuration
        if args.config:
            config = Config.from_file(args.config)
        elif args.env:
            config = Config.from_env()
        else:
            config = Config()
        
        print("Current Configuration")
        print("=" * 50)
        for key, value in config.to_dict().items():
            print(f"  {key}: {value}")
    
    elif args.action == "validate":
        # Validate configuration
        try:
            if args.config:
                config = Config.from_file(args.config)
            elif args.env:
                config = Config.from_env()
            else:
                config = Config()
            
            config.validate()
            print("Configuration is valid!")
        except ValueError as e:
            print(f"Configuration error: {e}", file=sys.stderr)
            sys.exit(1)
    
    elif args.action == "generate":
        # Generate a sample configuration file
        sample_config = {
            "max_workers": 4,
            "queue_size": 1000,
            "worker_timeout": 300,
            "task_timeout": 60,
            "retry_attempts": 3,
            "log_level": "INFO",
            "heartbeat_interval": 60,
        }
        
        output_path = args.output or "pytaskq_config.yaml"
        
        with open(output_path, "w") as f:
            yaml.dump(sample_config, f, default_flow_style=False)
        
        print(f"Sample configuration written to {output_path}")


def cmd_metrics(args: argparse.Namespace) -> None:
    """
    Show metrics for the task queue.

    Args:
        args: Parsed command-line arguments.
    """
    setup_logging(args.log_level)
    
    # Import here to avoid issues
    from pytaskq import MetricsCollector
    
    # Create metrics collector
    metrics = MetricsCollector()
    
    print("Task Queue Metrics")
    print("=" * 50)
    
    # Get current snapshot
    snapshot = metrics.snapshot()
    
    print(f"Tasks submitted: {snapshot.tasks_submitted}")
    print(f"Tasks completed: {snapshot.tasks_completed}")
    print(f"Tasks failed: {snapshot.tasks_failed}")
    print(f"Average duration: {snapshot.average_duration:.4f} seconds")
    
    # Calculate success rate
    total_executed = snapshot.tasks_completed + snapshot.tasks_failed
    if total_executed > 0:
        success_rate = (snapshot.tasks_completed / total_executed) * 100
        print(f"Success rate: {success_rate:.2f}%")
    else:
        print("Success rate: N/A (no tasks executed)")


def cmd_version(args: argparse.Namespace) -> None:
    """
    Show version information.

    Args:
        args: Parsed command-line arguments.
    """
    # Import version
    import pytaskq
    print(f"PyTaskQ version {pytaskq.__version__}")


def create_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="pytaskq",
        description="PyTaskQ - Python Task Queue Library v2",
        epilog="Use 'pytaskq <command> --help' for more information on a specific command.",
    )
    
    # Global options
    parser.add_argument(
        "-v", "--version",
        action="store_true",
        help="Show version information and exit"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)"
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(
        dest="command",
        title="Commands",
        description="Available commands",
        help="Command to execute"
    )
    
    # Worker command
    worker_parser = subparsers.add_parser(
        "worker",
        help="Start a worker process"
    )
    worker_parser.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of worker threads to start"
    )
    worker_parser.add_argument(
        "-c", "--config",
        help="Path to configuration file (YAML)"
    )
    worker_parser.add_argument(
        "-s", "--storage",
        help="Path to SQLite storage database"
    )
    worker_parser.add_argument(
        "--metrics-interval",
        type=int,
        help="Interval in seconds to log metrics (default: disabled)"
    )
    worker_parser.set_defaults(func=cmd_worker)
    
    # Submit command
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a task to the queue"
    )
    submit_parser.add_argument(
        "name",
        help="Name of the task"
    )
    submit_parser.add_argument(
        "-p", "--priority",
        type=int,
        choices=range(0, 11),
        help="Task priority (0-10, 0 is highest)"
    )
    submit_parser.add_argument(
        "-m", "--metadata",
        help="Task metadata as JSON string"
    )
    submit_parser.add_argument(
        "-s", "--storage",
        help="Path to SQLite storage database"
    )
    submit_parser.set_defaults(func=cmd_submit)
    
    # Status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show queue status"
    )
    status_parser.add_argument(
        "-s", "--storage",
        help="Path to SQLite storage database"
    )
    status_parser.set_defaults(func=cmd_status)
    
    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Manage configuration"
    )
    config_parser.add_argument(
        "action",
        choices=["show", "validate", "generate"],
        help="Configuration action"
    )
    config_parser.add_argument(
        "-c", "--config",
        help="Path to configuration file (YAML)"
    )
    config_parser.add_argument(
        "-e", "--env",
        action="store_true",
        help="Load configuration from environment variables"
    )
    config_parser.add_argument(
        "--output",
        help="Output path for generated configuration file"
    )
    config_parser.set_defaults(func=cmd_config)
    
    # Metrics command
    metrics_parser = subparsers.add_parser(
        "metrics",
        help="Show task queue metrics"
    )
    metrics_parser.set_defaults(func=cmd_metrics)
    
    # Version command
    version_parser = subparsers.add_parser(
        "version",
        help="Show version information"
    )
    version_parser.set_defaults(func=cmd_version)
    
    return parser


def main() -> int:
    """
    Main entry point for the CLI.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = create_parser()
    args = parser.parse_args()
    
    # Handle version flag
    if args.version:
        cmd_version(args)
        return 0
    
    # Ensure a subcommand was provided
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute the requested command
    try:
        args.func(args)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.log_level == "DEBUG":
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())