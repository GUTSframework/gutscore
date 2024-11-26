"""Tests for the gutscore.task module."""

import pytest
from scheduler.task import Task

def test_define_blank_task():
    """Define an empty task."""
    task = Task("blank", {})
    assert task.to_json() == '{"function_name": "blank", "args": {}}'

def test_execute_blank_task():
    """Try executing an undefined function task."""
    task = Task("blank", {})
    with pytest.raises(Exception):
        task.execute()

def test_execute_napping_task():
    """Execute a task using the nap_test function."""
    task = Task("nap_test", {"nap_duration": 0.1})
    task.execute()
