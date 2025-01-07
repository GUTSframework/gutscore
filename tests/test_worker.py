"""Tests for the gutscore.worker functions."""

import pytest
from scheduler.queue import Queue
from scheduler.event import Event
from scheduler.task import Task
from scheduler.worker import worker_function


def test_worker_function_no_queue(caplog : pytest.LogCaptureFixture):
    """Test worker_function call."""
    worker_function(None, 0, 0, 1, 1)
    assert "runtime exceeded 1" in caplog.text

def test_worker_function_with_queue():
    """Test worker_function call with queue."""
    queue = Queue()
    for _ in range(2):
        queue.add_task(Task("nap_test", {"nap_duration": 0.1}))
    worker_function(queue, 0, 0, 4, 1)
    assert queue.get_completed_tasks() == 2
    queue.delete(timeout=0.5)

def test_worker_function_with_queue_maxtask():
    """Test worker_function call with queue, cap number of task."""
    queue = Queue()
    for _ in range(2):
        queue.add_task(Task("nap_test", {"nap_duration": 0.1}))
    worker_function(queue, 0, 0, 1, 1)
    assert queue.get_completed_tasks() == 1
    queue.delete(timeout=0.5)

def test_worker_function_with_queue_and_kill():
    """Test worker_function call with queue and kill event."""
    queue = Queue()
    queue.add_event(Event(eid = 1, action ="worker-kill", target = "all"))
    with pytest.raises(SystemExit):
        worker_function(queue, 0, 0, 1, 1)
    queue.delete(timeout=0.5)

def test_worker_function_with_queue_event():
    """Test worker_function call with queue, event with different target."""
    queue = Queue()
    queue.add_event(Event(eid = 1, action ="worker-kill", target = "{0,2}"))
    worker_function(queue, 0, 0, 1, 1)
    assert queue.get_events_count() == 1
    queue.delete(timeout=0.5)
