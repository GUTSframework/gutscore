"""Tests for the gutscore.task module."""

import pytest
from scheduler.task import Task
from scheduler.task import register_taskf
from scheduler.task import unregister_taskf


def test_define_blank_task():
    """Define an empty task."""
    task = Task("blank", {})
    assert task.to_json() == '{"function_name": "blank", "args": {}}'

def test_execute_blank_task():
    """Try executing an undefined function task."""
    task = Task("blank", {})
    with pytest.raises(ValueError):
        task.execute()

def test_execute_napping_task():
    """Execute a task using the nap_test function."""
    task = Task("nap_test", {"nap_duration": 0.1})
    task.execute()

def test_execute_failing_task():
    """Execute a task using the failing_test function."""
    task = Task("fail_test", {})
    with pytest.raises(RuntimeError):
        task.execute()

def test_add_remove_taskf_to_register():
    """Add a local task function to the register."""
    @register_taskf("local_test_function")
    def local_test_function() -> None:
        """A local test function."""
    task = Task("local_test_function", {})
    task.execute()
    unregister_taskf("local_test_function")
