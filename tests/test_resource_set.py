"""Tests for the scheduler.resource_set class."""
import pytest
from scheduler.resource_set import ResourceSetLocal
from scheduler.resource_set import ResourceSetSlurm


def test_define_local_resource_set():
    """Test initializing a local resource set."""
    res_config = {"nworkers": 2, "runtime": "10"}
    res = ResourceSetLocal(15, res_config = res_config)
    assert res.get_nworkers() == 2

def test_serialize_local_resource_set():
    """Test initializing a local resource set."""
    res_config = {"nworkers": 2, "runtime": "10"}
    res = ResourceSetLocal(10000, res_config = res_config)
    serialized_res = res.serialize()
    res_restored = ResourceSetLocal(10000, json_str = serialized_res)
    assert res_restored.get_nworkers() == 2

@pytest.mark.usefixtures("slurm_available")
def test_define_slurm_resource_set():
    """Test initializing a slurm resource set."""
    res_config = {"nworkers": 7, "nodes": 1, "runtime": "30"}
    res = ResourceSetSlurm(12, res_config = res_config)
    assert res.get_nworkers() == 7

@pytest.mark.usefixtures("slurm_available")
def test_serialize_slurm_resource_set():
    """Test serializing a slurm resource set."""
    res_config = {"nworkers": 12, "nodes": 1, "runtime": "1:30"}
    res = ResourceSetSlurm(12, res_config = res_config)
    serialized_res = res.serialize()
    res_restored = ResourceSetSlurm(12, json_str = serialized_res)
    assert res_restored.get_nworkers() == 12
