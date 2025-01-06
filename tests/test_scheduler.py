"""Tests for the gutscore scheduler class."""
import time
from pathlib import Path
import numpy as np
import psutil
import pytest
import toml
from scheduler.scheduler import Scheduler
from scheduler.task import Task


def test_init_scheduler():
    """Test creating a scheduler."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test"}}, f)
    scheduler = Scheduler(a_args=[])
    assert (scheduler.name() == "test")
    assert Path("test_queue.db").exists() is True
    scheduler.cleanup()

def test_init_scheduler_wgroup_spawn():
    """Test creating a scheduler while spawning a workergroup."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test_spawn"},
                   "WG00042" : {"NullField" : 0}}, f)
    scheduler = Scheduler(a_args=["-i","input.toml","-wg","42"])
    assert (scheduler.name() == "test_spawn")
    assert (scheduler.wgroup_id_respawn() == 42)
    scheduler.cleanup()

def test_start_scheduler():
    """Test starting a scheduler."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test"},
                   "resource": {"nwgroups": 2,
                                "config": {"nworkers": 1, "runtime": 1}}}, f)
    scheduler = Scheduler(a_args=[])
    scheduler.start()
    assert scheduler.get_queue().get_worker_groups_count() == 2
    time.sleep(1)
    scheduler.cleanup()

def test_run_wgroup_scheduler_fail_nospawn():
    """Test spawning wgroup fail no input section."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test"},
                   "resource": {"nwgroups": 2,
                                "config": {"nworkers": 1, "runtime": 1}}}, f)
    scheduler = Scheduler(a_args=[])
    with pytest.raises(ValueError):
        scheduler.run_wgroup()
    scheduler.cleanup()

def test_run_wgroup_scheduler_fail_wrong_gid():
    """Test spawning wgroup fail GID."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test"},
                   "WG00042" : {"NullField" : 0},
                   "resource": {"nwgroups": 2,
                                "config": {"nworkers": 1, "runtime": 1}}}, f)
    scheduler = Scheduler(a_args=["-i","input.toml","-wg","42"])
    with pytest.raises(ValueError):
        scheduler.run_wgroup()
    scheduler.cleanup()

def test_oversubscribe_scheduler():
    """Test oversubscribing a scheduler."""
    ncpus = int(np.ceil(psutil.cpu_count()/4)) + 1
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test_oversub"},
                   "resource": {"nwgroups": 4,
                                "config": {"nworkers": ncpus, "runtime": 1}}}, f)
    scheduler = Scheduler(a_args=[])
    with pytest.raises(RuntimeError):
        scheduler.start()
    scheduler.cleanup()

def test_start_scheduler_with_tasks():
    """Test starting a scheduler and giving it tasks."""
    with Path("input.toml").open("w") as f:
        toml.dump({"case": {"name": "test"},
                   "resource": {"nwgroups": 2,
                                "config": {"nworkers": 1, "runtime": 10}}}, f)
    scheduler = Scheduler(a_args=[])
    scheduler.start()
    for _ in range(10):
        scheduler.add_task(Task("nap_test", {"nap_duration": 0.1}))
    time.sleep(2)
    assert scheduler.get_queue().get_completed_tasks() == 10
    scheduler.cleanup()
