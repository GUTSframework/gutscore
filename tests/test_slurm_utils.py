"""Tests for the scheduler.slurm_utils functions."""
import pytest
from scheduler.slurm_utils import SlurmCluster
from scheduler.slurm_utils import is_slurm_avail
from scheduler.slurm_utils import time_to_s


@pytest.mark.usefixtures("slurm_not_available")
def test_donot_have_slurm():
    """Test the slurm avail function, when no slurm."""
    has_slurm = is_slurm_avail()
    assert not has_slurm

@pytest.mark.usefixtures("slurm_available")
def test_has_slurm():
    """Test the slurm avail function."""
    has_slurm = is_slurm_avail()
    assert has_slurm

def test_time_util():
    """Test time format convertion util."""
    time_s = time_to_s("00:00:30")
    assert time_s == 30
    time_s = time_to_s("00:05:12")
    assert time_s == 312
    time_s = time_to_s("05:12")
    assert time_s == 312
    time_s = time_to_s("10:05:56")
    assert time_s == 36356
    with pytest.raises(ValueError):
        _ = time_to_s("W-30:10")
    with pytest.raises(ValueError):
        _ = time_to_s("1-30:10:30:1")
    with pytest.raises(ValueError):
        _ = time_to_s("300:10:30")

@pytest.mark.usefixtures("slurm_available")
def test_init_slurmcluster():
    """Test initializing a SlurmCluster."""
    slurm_cluster = SlurmCluster()
    assert slurm_cluster.get_node_count() > 0

@pytest.mark.usefixtures("slurm_available")
def test_failcheck_res_config():
    """Test for resource config errors."""
    slurm_cluster = SlurmCluster()
    res_config = {"partition": "UnlikelyName"}          # Wrong partition
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1}                           # Missing time
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1, "time": "01:100:00"}      # Wrong time format
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1000000, "time": "01:00:00"} # Too many nodes
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)

@pytest.mark.usefixtures("slurm_available")
def test_success_check_res_config():
    """Test for resource config succeful processing."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "01:00:00"}
    up_config = slurm_cluster.process_res_config(res_config)
    assert res_config["nodes"] == up_config["nodes"]
