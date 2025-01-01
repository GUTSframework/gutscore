"""Tests for the gutscore.workergroup class."""

import pytest
from scheduler.queue import Queue
from scheduler.resource_manager import ResourceManager
from scheduler.workergroup import WorkerGroup


def test_init():
    """Test creating a workergroup."""
    config = {}
    res_config = {}
    wgroup = WorkerGroup(0, config, res_config)
    assert(wgroup.id() == 0)

def test_init_withqueue():
    """Test creating a workergroup with a queue."""
    queue = Queue()
    config = {}
    res_config = {}
    wgroup = WorkerGroup(12, config, res_config, queue = queue)
    assert(wgroup.id() == 12)
    assert queue.get_worker_groups_count() == 1
    queue.delete(timeout=2)

def test_attach_queue():
    """Test creating a workergroup then attach a queue."""
    config = {}
    res_config = {}
    wgroup = WorkerGroup(42, config, res_config)
    queue = Queue()
    wgroup.attach_queue(queue)
    assert(wgroup.id() == 42)
    assert queue.get_worker_groups_count() == 1
    queue.delete(timeout=2)

def test_reattach_queue():
    """Test creating a workergroup with a queue then attach a queue."""
    queue_1 = Queue()
    config = {}
    res_config = {}
    wgroup = WorkerGroup(0, config, res_config, queue = queue_1)
    queue_2 = Queue() # In practice, same as queue_1
    with pytest.raises(RuntimeError):
        wgroup.attach_queue(queue_2)
    queue_1.delete(timeout=2)

def test_request_resources_without_queue():
    """Test requiring resources without a queue."""
    config = {}
    res_config = {}
    wgroup = WorkerGroup(0, config, res_config)
    manager = ResourceManager(config)
    with pytest.raises(RuntimeError):
        wgroup.request_resources(manager)

def test_launch_without_queue():
    """Test launching without a queue."""
    config = {}
    res_config = {}
    wgroup = WorkerGroup(0, config, res_config)
    manager = ResourceManager(config)
    with pytest.raises(RuntimeError):
        wgroup.launch(manager)

def test_request_resources_with_queue_erroneous_res():
    """Test requiring resources with a queue, erroneous resources."""
    # Setup wgroup
    config = {}
    res_config = {"nworkers": 10000}
    wgroup = WorkerGroup(0, config, res_config)
    # Attach queue
    queue = Queue("myqueue.db")
    wgroup.attach_queue(queue)
    manager = ResourceManager(config)
    with pytest.raises(RuntimeError):
        wgroup.request_resources(manager)
    queue.delete(timeout=2)
