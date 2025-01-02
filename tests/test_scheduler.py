"""Tests for the gutscore scheduler class."""
from pathlib import Path
import toml
from scheduler.scheduler import Scheduler


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
