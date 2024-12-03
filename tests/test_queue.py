"""Tests for the scheduler.Queue class."""
import uuid
from pathlib import Path
import pytest
from scheduler.event import Event
from scheduler.queue import Queue
from scheduler.task import Task


def test_init_queue():
    """Test creating a database."""
    queue = Queue()
    assert queue.file_name() == "guts_queue.db"
    Path("./guts_queue.db").unlink(missing_ok=True)

def test_init_named_queue():
    """Test creating a database with a custom name."""
    queue = Queue("myguts_queue.db")
    assert queue.file_name() == "myguts_queue.db"
    Path("./myguts_queue.db").unlink(missing_ok=True)

def test_delete_queue():
    """Test deleting the queue."""
    queue = Queue()
    queue.delete(timeout=2)

def test_add_task():
    """Test adding a task to the queue."""
    queue = Queue()
    task = Task("dummy", {})
    queue.add_task(task)
    assert queue.get_remaining_tasks_count() == 1
    queue.delete(timeout=0.5)

def test_add_task_with_deps():
    """Test adding a task with dependencies to the queue."""
    queue = Queue()
    task = Task("dummy", {})
    task_uuid = queue.add_task(task)
    queue.add_task(task, task_uuid)
    assert queue.get_remaining_tasks_count() == 2
    queue.delete(timeout=0.5)

def test_add_task_with_unknown_deps():
    """Test adding a task with unknown dependencies to the queue."""
    queue = Queue()
    task = Task("dummy", {})
    queue.add_task(task)
    random_uuid = uuid.uuid4()
    with pytest.raises(RuntimeError):
        queue.add_task(task, random_uuid)
    queue.delete(timeout=1)

def test_mark_task_done():
    """Test marking a task as done."""
    queue = Queue()
    task_id = queue.add_task(Task("dummy", {}))
    queue.mark_task_done(task_id)
    assert queue.get_tasks_count() == 1
    assert queue.get_remaining_tasks_count() == 0
    queue.delete(timeout=0.2)

def test_mark_task_inprogress():
    """Test marking a task as inprogress."""
    queue = Queue()
    _ = queue.add_task(Task("dummy", {}))
    _ = queue.fetch_task()
    assert queue.get_running_tasks_count() == 1
    queue.delete(timeout=0.2)

def test_add_event():
    """Test adding an event to the queue."""
    queue = Queue()
    event = Event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    assert queue.get_events_count() == 1
    queue.delete(timeout=1)

def test_fetch_event():
    """Test fetching an event in the queue."""
    queue = Queue()
    event = Event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    _ = queue.fetch_event()
    assert queue.get_events_count() == 1
    queue.delete(timeout=1)

def test_fetch_event_counter():
    """Test fetching multiple times an event in the queue."""
    queue = Queue()
    event = Event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    _ = queue.fetch_event()
    _ = queue.fetch_event()
    _, count, _ = queue.fetch_event()
    assert count == 3
    queue.delete(timeout=1)

def test_register_worker():
    """Test registering a worker."""
    queue = Queue()
    queue.register_worker((0,0))
    queue.register_worker((0,1))
    assert queue.get_workers_count() == 2
    queue.delete(timeout=0.2)

def test_unregister_worker():
    """Test unregistering a worker."""
    queue = Queue()
    queue.register_worker((0,0))
    queue.unregister_worker((0,0))
    assert queue.get_workers_count() == 0
    queue.delete(timeout=0.2)

def test_update_worker_status():
    """Test registering a worker."""
    queue = Queue()
    queue.register_worker((0,0))
    queue.update_worker_status((0,0), "working")
    assert queue.get_active_workers_count() == 1
    queue.delete(timeout=0.2)
