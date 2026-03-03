# PyTaskQ - Python Task Queue Library v2

A lightweight, thread-safe task queue implementation in Python using heapq for priority-based task scheduling and threading for concurrent execution.

## Features

- Priority-based task scheduling using heapq
- Thread-safe concurrent execution using threading
- YAML-based configuration
- Worker pool management
- Simple and intuitive API

## Installation

```bash
pip install pytaskq
```

## Development Installation

```bash
pip install -e ".[dev]"
```

## Usage

```python
from pytaskq import TaskQueue, Task

# Create a task queue
queue = TaskQueue()

# Define a task
@queue.task(priority=1)
def my_task():
    # Do some work
    pass

# Start processing tasks
queue.start()
```

## Testing

Run tests with pytest:

```bash
pytest
```

## Project Structure

```
pytaskq/
├── __init__.py
├── queue.py       # Task queue implementation
├── task.py        # Task class
└── worker.py      # Worker pool implementation

tests/
├── __init__.py
├── test_queue.py
├── test_task.py
└── test_worker.py
```

## License

MIT License