"""Tests for the scheduler.slurm_utils functions."""
import time
import pytest
from scheduler.slurm_utils import SlurmCluster
from scheduler.slurm_utils import cancel_slurm_job
from scheduler.slurm_utils import get_inqueue_slurm_jobs
from scheduler.slurm_utils import get_past_slurm_jobs
from scheduler.slurm_utils import is_slurm_avail
from scheduler.slurm_utils import make_job_script_wgroup
from scheduler.slurm_utils import submit_slurm_job
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
    res_config = {"nodes": 1, "runtime": "01:100:00"}      # Wrong time format
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1000000, "runtime": "01:00:00"} # Too many nodes
    with pytest.raises(ValueError):
        slurm_cluster.process_res_config(res_config)

@pytest.mark.usefixtures("slurm_available")
def test_success_check_res_config():
    """Test for resource config succeful processing."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "01:00:00"}
    up_config = slurm_cluster.process_res_config(res_config)
    assert res_config["nodes"] == up_config["nodes"]

@pytest.mark.usefixtures("slurm_available")
def test_build_script():
    """Test assembling a slurm script from resource config."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "00:05:00"}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    assert job_script is not None

@pytest.mark.usefixtures("slurm_available")
def test_submit_job():
    """Test submitting a Slurm job to the queue."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "00:01:00",
                  "extra_directives": {"--exclusive" : None,
                                       "--hold": None}}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    job_id = submit_slurm_job(0, job_script)
    assert job_id is not None
    cancel_slurm_job(job_id)

@pytest.mark.usefixtures("slurm_available")
def test_submit_and_query_job():
    """Test submitting a Slurm job to the queue and querying slurm."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "00:01:00",
                  "extra_directives": {"--exclusive" : None,
                                       "--hold": None}}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    job_id = submit_slurm_job(0, job_script)
    assert job_id is not None
    running_jobs = get_inqueue_slurm_jobs()
    assert any(d["id"] == job_id for d in running_jobs)
    cancel_slurm_job(job_id)

@pytest.mark.usefixtures("slurm_available")
def test_submit_and_cancel_job():
    """Test submitting and deleting a Slurm job to/from the queue."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "time": "00:01:00",
                  "extra_directives": {"--exclusive" : None}}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    job_id = submit_slurm_job(0, job_script)
    assert job_id is not None
    cancel_slurm_job(job_id)
    # Time needed for slurm to update its internals
    time.sleep(5.0)
    past_jobs = get_past_slurm_jobs()
    assert any(d["id"] == job_id for d in past_jobs)
