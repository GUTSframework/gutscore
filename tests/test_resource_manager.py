"""Tests for the scheduler.resource_manager class."""
import json
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

def test_get_resource_local():
    """Test requesting a local resource set from manager."""
    config = {"resource" : {"nwgroups": 1}}
    manager = ResourceManager(config)
    res_config = {"nworkers": 1}
    local_res = manager.get_resources(res_config, 0)
    assert json.loads(local_res.serialize())["wgroup_CLI_pid"] != -1

@pytest.mark.usefixtures("slurm_available")
def test_get_resource_slurm():
    """Test requesting a slurm resource set from manager."""
    config = {"resource" : {"nwgroups": 1, "backend" : "slurm"}}
    manager = ResourceManager(config)
    res_config = {"nworkers": 1, "nodes": 1, "runtime": "1:30"}
    slurm_res = manager.get_resources(res_config, 0)
    assert json.loads(slurm_res.serialize())["job_id"] is not None

@pytest.mark.usefixtures("slurm_available")
def test_oversubscribe_resources_slurm():
    """Test oversubscribing a slurm with a large number of nodes."""
    config = {"resource" : {"nwgroups": 1, "backend" : "slurm"}}
    manager = ResourceManager(config)
    res_config = {"nworkers": 12, "nodes": 100000, "runtime": "1:30"}
    with pytest.raises(ValueError):
        _ = manager.get_resources(res_config, 0)

@pytest.mark.usefixtures("slurm_not_available")
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
