"""Tests for the scheduler.resource_manager class."""
import pytest
from scheduler.resource_manager import ResourceManager
from scheduler.sys_utils import get_cpu_count


def test_init_manager():
    """Test creating a resource manager."""
    config = {"resource" : {"nwgroups": 2}}
    manager = ResourceManager(config)
    assert(manager.get_nwgroups() == 2)

def test_init_manager_unknown_backend():
    """Test creating a resource manager with unknown backend."""
    config = {"resource" : {"nwgroups": 1, "backend" : "WillFail"}}
    manager = ResourceManager(config)
    with pytest.raises(ValueError):
        _ = manager.get_resources({}, 0)

def test_oversubscribe_resources():
    """Test getting too many resources from the manager."""
    config = {"resource" : {"nwgroups": 1}}
    manager = ResourceManager(config)
    res_config = {"nworkers": get_cpu_count() + 1}
    with pytest.raises(RuntimeError):
        _ = manager.get_resources(res_config, 0)

def test_slurm_withoutslurm():
    """Test creating a resource manager without SLURM."""
    config = {"resource" : {"nwgroups": 1, "backend" : "slurm"}}
    with pytest.raises(RuntimeError):
        _ = ResourceManager(config)

def test_instanciate_backendmix():
    """Test creating a resource manager with a mix of backends."""
    config = {"resource" : {"nwgroups": 1}}
    manager = ResourceManager(config)
    json_res = '{"type":"slurm"}'
    with pytest.raises(ValueError):
        _ = manager.instanciate_resource(json_res)
