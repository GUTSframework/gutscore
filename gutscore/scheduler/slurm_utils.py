"""A set of SLURM utilities for gutscore."""
from __future__ import annotations
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any
from scheduler.sys_utils import get_username

_logger = logging.getLogger(__name__)

_slurm_dir_prefix : str = "#SBATCH"
_slurm_header : list[str] = ["#!/bin/bash"]
_slurm_group_cmd : str  = "run_workergroup"

def is_slurm_avail() -> bool:
    """Assess if slurm is available on system.

    Detect if Slurm is available in the compute environment.
    First checking for environment variable, then checking
    the `sinfo` command.

    Returns:
        A boolean True if Slurm is detected, False otherwise
    """
    _sinfo_cmd = ["sinfo", "--version"]
    slurm_avail = False

    # Test environment variable
    if "SLURM_VERSION" in os.environ:
        slurm_avail = True

    # Test for a slurm command
    try:
        result = subprocess.run(_sinfo_cmd,
                                capture_output=True,
                                check=True)
        if result.returncode == 0:
            slurm_avail = True
        else:
            if slurm_avail:
                _logger.warning("SLURM sinfo is not available even though SLURM_VERSION is defined")
            slurm_avail = False
    except FileNotFoundError:
        if slurm_avail:
            _logger.warning("SLURM sinfo is not available even though SLURM_VERSION is defined")
        slurm_avail = False

    return slurm_avail

def time_to_s(slurm_time : str) -> int:
    """Convert a Slurm formatted time to seconds.

    Arguments:
        slurm_time : A Slurm formatted time d-hh:mm:ss

    Returns:
        Time converted to seconds

    Raises:
        ValueError : If the input string is not properly formatted
    """
    ndays = "0"
    if slurm_time.find("-") == -1:
        hms = slurm_time.split(":")
    else:
        ndays, intra_day = slurm_time.split("-")
        hms = intra_day.split(":")

    standard_len = [1, 2, 3]

    # Checks
    if len(hms) > standard_len[2]:
        err_msg = f"Format error: {hms} does not match hh:mm:ss"
        raise ValueError(err_msg)
    for e in hms:
        if len(e) > standard_len[1] or not e.isdigit():
            err_msg = f"Format error: {hms} does not match hh:mm:ss"
            raise ValueError(err_msg)
    if not ndays.isdigit():
        err_msg = f"Format error: {ndays} is not a number in d-hh:mm:ss"
        raise ValueError(err_msg)

    # Add days
    time = int(ndays) * 60 * 3600 * 24

    # Aggretates hours, minutes and seconds
    if len(hms) == standard_len[0]:
        time = time + int(hms[0])
    elif len(hms) == standard_len[1]:
        time = time + int(hms[1])
        time = time + 60 * int(hms[0])
    elif len(hms) == standard_len[2]:
        time = time + int(hms[2])
        time = time + 60 * int(hms[1])
        time = time + 3600 * int(hms[0])

    return time

class SlurmCluster:
    """A class to describe the Slurm HPC Cluster compute resources.

    Attributes:
        _sinfo_executable : Name or path to the sinfo executable, by default "sinfo".
        _sacct_executable : Name or path to the sacct executable, by default "sacct".
        _all_nodes : The list of nodes available
        _all_partitions : The list of partitions available with number of nodes
        _default_partition : Name of the default partition
        _all_cpu_per_nodetypes : Number of CPUs available on each type of nodes
        _all_gpu_per_nodetypes : Number of GPUs available on each type of nodes
    """

    _sinfo_executable : str = "sinfo"
    _sacct_executable : str = "sacct"

    def __init__(self) -> None:
        """SlurmCluster initialize function.

        Raises:
            RuntimeError : If Slurm is not available on the system during initalization
        """
        if not is_slurm_avail():
            err_msg = "Trying to initialize SlurmCluster without SLURM."
            raise RuntimeError(err_msg)

        self._all_nodes : list[str] = self._list_all_nodes()
        self._all_part_nnodes, self._all_part_maxtimes, \
            self._default_partition = self._list_all_partitions()
        self._all_cpu_per_nodetypes : dict[str,int] = self._list_all_partition_cpus()
        self._all_gpu_per_nodetypes : dict[str,int] = self._list_all_partition_gpus()

    def _list_all_partitions(self) -> tuple[dict[str,int],dict[str,int],str | None]:
        """Query Slurm to get the list of partitions.

        Returns:
            a dict total number of nodes for each partition
            a dict of max runtime for each partition
            the default partition
        """
        nnodes_dict = {}
        mtimes_dict = {}
        sinfo_cmd = f"{self._sinfo_executable} --noheader --format='%P %F %l'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        partitions_ncount_list = sinfo_out.split("\n")[:-1]
        # Find the default if defined, total number of nodes per partition
        default_partition = None
        for p in range(len(partitions_ncount_list)):
            partition, ncount_s, mtimes_s = partitions_ncount_list[p].split(" ")
            if partition[-1] == "*":
                partition = partition[:-1]
                default_partition = partition
            ncount = ncount_s.split("/")[3]
            nnodes_dict[partition] = int(ncount)
            mtimes_dict[partition] = time_to_s(mtimes_s.strip())
        return nnodes_dict, mtimes_dict, default_partition

    def _list_all_nodes(self) -> list[str]:
        """Query Slurm to get the list of nodes.

        Returns:
            the list of nodes on the system
        """
        sinfo_cmd = f"{self._sinfo_executable} --noheader --format='%n'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        return sinfo_out.split("\n")[:-1]

    def _list_all_partition_cpus(self) -> dict[str,int]:
        """Query Slurm to get number of CPUs for each partition.

        Returns:
            A dictionary with the number of CPU for each partition name
        """
        cpus_dict = {}
        sinfo_cmd = f"{self._sinfo_executable} --noheader --format='%P %c'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        ncpu_list = sinfo_out.split("\n")[:-1]
        for part_cpu in ncpu_list:
            part, cpu = part_cpu.split(" ")
            if part[-1] == "*":
                part = part[:-1]
            cpus_dict[part] = int(cpu)
        return cpus_dict

    def _list_all_partition_gpus(self) -> dict[str,int]:
        """Query Slurm to get number of GPUs for each partition.

        Returns:
            A dictionary with the number of GPU for each partition name
        """
        gpus_dict = {}
        sinfo_cmd = f"{self._sinfo_executable} --noheader --format='%P %G'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        gres_list = sinfo_out.split("\n")[:-1]
        for part_gres in gres_list:
            ngpus = 0
            part, gres = part_gres.split(" ")
            if part[-1] == "*":
                part = part[:-1]
            # Count the total number of GPUs, maybe multiple type
            # of GPUs on a single node
            if "gpu:" in gres:
                gres_l = gres.split(",")
                for g in gres_l:
                    if "gpu:" in g:
                        ngpus += int(g.split(":")[2][0])
            gpus_dict[part] = ngpus
        return gpus_dict

    def get_node_count(self) -> int:
        """Get the number of nodes on the system.

        Returns:
            number of nodes on the system
        """
        return len(self._all_nodes)

    def get_partition_count(self) -> int:
        """Get the number of partitions on the system.

        Returns:
            number of partitions on the system
        """
        return len(self._all_part_nnodes)

    def process_res_config(self, res_config : dict[Any,Any]) -> dict[Any,Any]:
        """Process the input config dictionary.

        Check the sanity of the keys contained in the dictionary
        and append identified missing keys with defaults.

        Args:
            res_config : A dictionary listing resource configuration

        Returns:
            An updated dictionary listing resource configuration

        Raises:
            ValueError : If a configuration key as a wrong value (partition, node, ...)
        """
        # Check the sanity of the provided parameters
        self._check_res_config(res_config)
        # Append default parameters
        return self._update_res_config(res_config)

    def _check_res_config(self, res_config : dict[Any,Any]) -> None:
        """Check the sanity of the user-provided parameters.

        Args:
            res_config : A dictionary listing resource configuration

        Raises:
            ValueError : If a configuration key as a wrong value (partition, node, ...)
        """
        # Check partition name if provided
        partition = res_config.get("partition")
        if partition and partition not in self._all_part_nnodes:
            err_msg = f"Partition {partition} unknown!"
            raise ValueError(err_msg)

        # Assume the default partition hereafter
        if not partition:
            partition = self._default_partition

        if not partition:
            err_msg = "Partition not provided and system has no default!"
            raise ValueError(err_msg)

        # Check time format and limit
        time = res_config.get("time")
        if not time:
            err_msg = "No runtime specified ! Use d-hh:mm:ss Slurm format"
            raise ValueError(err_msg)
        time_s = time_to_s(time)
        if time_s > self._all_part_maxtimes[partition]:
            err_msg = f"Requested runtime {time} exceeds partition limit!"
            raise ValueError(err_msg)

        # Slurm has a complex rationale when it comes to
        # nodes and cpus, just catch obvious errors here
        nnodes = res_config.get("nodes")
        if nnodes and nnodes > self._all_part_nnodes[partition]:
            err_msg = f"Requested number of nodes {nnodes} exceeds partition max count!"
            raise ValueError(err_msg)

        ngpus = res_config.get("gpus-per-node")
        if ngpus and ngpus > self._all_gpu_per_nodetypes[partition]:
            err_msg = f"Requested number of GPUs {ngpus} per node exceeds the node limit!"
            raise ValueError(err_msg)


    def _update_res_config(self, res_config : dict[Any,Any]) -> dict[Any,Any]:
        """Update resource configuration parameters with defaults.

        Args:
            res_config :A dictionary listing resource configuration

        Returns:
            An updated dictionary listing resource configuration
        """
        updated_config = res_config
        partition = res_config.get("partition")
        if not partition:
            updated_config["partition"] = self._default_partition

        return updated_config

def make_job_script_wgroup(wgroup_id : int,
                           res_config: dict[Any,Any]) -> list[str]:
    """Assemble a workergroup job script from resource config.

    Args:
        wgroup_id : The workergroup index number
        res_config : The resource configuration specification

    Returns:
        A full Slurm batch script as a list of strings, one per line
    """
    # Initialize with header
    job_script = list(_slurm_header)
    job_script.append(f"{_slurm_dir_prefix} --job-name=GUTS_WG{wgroup_id:05d}")

    # Append mandatory directives
    runtime = res_config.get("time")
    partition = res_config.get("partition")
    nnodes = res_config.get("nodes")
    job_script.append(f"{_slurm_dir_prefix} --time={runtime}")
    job_script.append(f"{_slurm_dir_prefix} --partition={partition}")
    job_script.append(f"{_slurm_dir_prefix} --nodes={nnodes}")

    # Account can be default associated to user in SLURM
    account = res_config.get("account")
    if account is not None:
        job_script.append(f"{_slurm_dir_prefix} --account={account}")

    # Add any user-defined extra directives
    extra_dirs = res_config.get("extra_directives", {})
    for key, value in extra_dirs.items():
        if value:
            job_script.append(f"{_slurm_dir_prefix} {key}={value}")
        else:
            job_script.append(f"{_slurm_dir_prefix} {key}")

    # Add pre-command lines
    pre_cmd_list = res_config.get("pre_cmd_list", [])
    job_script.extend(pre_cmd_list)

    toml_file = f"input_WG{wgroup_id:05d}.toml"
    full_slurm_group_cmd = f"{_slurm_group_cmd} -i {toml_file} -wg {wgroup_id}"
    job_script.append(full_slurm_group_cmd)

    post_cmd_list = res_config.get("post_cmd_list", [])
    job_script.extend(post_cmd_list)

    return job_script

def submit_slurm_job(wgroup_id : int,
                     job_script : list[str]) -> int:
    """Submit a job to the Slurm queue.

    Args:
        wgroup_id : The workergroup index
        job_script : The job batch script as a list of strings

    Returns:
        The submitted SLURM_JOB_ID

    Raises:
        RuntimeError : If it fails to submit the batch script
    """
    sbatch = "sbatch"

    # Dump script to temporary file
    tmp_batch = f".WG{wgroup_id:05d}.batch"
    with Path(tmp_batch).open("w") as f:
        for line in job_script:
            f.write(f"{line}\n")

    sbatch_cmd = [sbatch]
    sbatch_cmd.append(tmp_batch)
    try:
        result = subprocess.run(sbatch_cmd,
                                capture_output=True,
                                check=False)
        if result.returncode != 0:
            slurm_err = result.stderr.decode("utf-8")
            err_msg = f"Unable to submit job to Slurm queue. Error {slurm_err}"
            _logger.exception(err_msg)
            raise RuntimeError(err_msg)
        stdout = result.stdout.decode("utf-8")
        return int(stdout.split(" ")[3])

    except subprocess.CalledProcessError:
        err_msg = f"Unable to submit job to Slurm queue for wgroup {wgroup_id}"
        _logger.exception(err_msg)
        raise

def get_inqueue_slurm_jobs() -> list[dict[Any,Any]]:
    """Get the list of jobs currently in queue.

    Returns:
        list of currently running jobs with main job info
    """
    squeue = "squeue"
    user = get_username()

    job_list = []

    squeue_cmd = [squeue]
    squeue_cmd.append("-u")
    squeue_cmd.append(user)
    squeue_cmd.append("--format='%i %P %j %t %M %D'")
    try:
        result = subprocess.run(squeue_cmd, stdout=subprocess.PIPE, check=False)
        header_msg = "JOBID PARTITION"
        stdout = result.stdout.decode("utf-8")
        if header_msg not in stdout:
            slurm_err = result.stderr.decode("utf-8")
            err_msg = f"Unable to query currently running jobs. Error {slurm_err}"
            _logger.exception(err_msg)
            raise RuntimeError(err_msg)
        job_raw_list = stdout.split("\n")[1:-1]
        for job in job_raw_list:
            job_id, part, jname, status, rtime, nnode = job[1:-1].split(" ")
            job_list.append({"id" : int(job_id),
                             "partition" : part,
                             "name" : jname,
                             "status" : status,
                             "nnode" : int(nnode),
                             "runtime" : rtime})
    except subprocess.CalledProcessError:
        err_msg = "Unable to query job from Slurm queue"
        _logger.exception(err_msg)
        raise
    else:
        return job_list

    return job_list

def get_past_slurm_jobs() -> list[dict[Any,Any]]:
    """Get the list of jobs recently submitted.

    Returns:
        list of past jobs with main job info
    """
    sacct = "sacct"
    user = get_username()

    job_list = []

    sacct_cmd = [sacct]
    sacct_cmd.append("-u")
    sacct_cmd.append(user)
    sacct_cmd.append("-X")
    sacct_cmd.append("-o")
    sacct_cmd.append("jobid,partition,jobname,state,time,nnodes")
    try:
        result = subprocess.run(sacct_cmd, stdout=subprocess.PIPE, check=False)
        header_msg = "JobID"
        stdout = result.stdout.decode("utf-8")
        if header_msg not in stdout:
            slurm_err = result.stderr.decode("utf-8")
            err_msg = f"Unable to query past jobs. Error {slurm_err}"
            _logger.exception(err_msg)
            raise RuntimeError(err_msg)
        job_raw_list = stdout.split("\n")[2:-1]
        for job in job_raw_list:
            clean_job = " ".join(job.split())
            job_id, part, jname, state, wtime, nnode = clean_job.split(" ")
            job_list.append({"id" : int(job_id),
                             "partition" : part,
                             "name" : jname,
                             "state" : state,
                             "nnode" : int(nnode),
                             "walltime" : wtime})
    except subprocess.CalledProcessError:
        err_msg = "Unable to query past job from Slurm queue"
        _logger.exception(err_msg)
        raise
    else:
        return job_list

    return job_list

def cancel_slurm_job(job_id : int) -> None:
    """Cancel a job.

    Args:
        job_id : the slurm id of the job to cancel
    """
    scancel = "scancel"

    # Get the list of jobs in queue
    inqueue_jobs = get_inqueue_slurm_jobs()
    is_running = any(d["id"] == job_id for d in inqueue_jobs)

    # Check jobs history
    is_past = False
    if not is_running:
        past_jobs = get_past_slurm_jobs()
        is_past = any(d["id"] == job_id for d in past_jobs)

    # Log a warning whern attempting to cancel finished job
    # do not raise error
    if is_past:
        warn_msg = f"Attempt to cancel already finished job {job_id}"
        _logger.warning(warn_msg)
        return

    # Do not raise error if job is neither running or past
    if not (is_past or is_running):
        warn_msg = f"Attempt to cancel already unknown job {job_id}"
        _logger.warning(warn_msg)

    # Try to cancel
    sbatch_cmd = [scancel]
    sbatch_cmd.append(str(job_id))
    try:
        result = subprocess.run(sbatch_cmd, stdout=subprocess.PIPE, check=False)
        if result.returncode != 0:
            slurm_err = result.stderr.decode("utf-8")
            err_msg = f"Unable to cancel job {job_id} from Slurm queue. Error {slurm_err}"
            _logger.exception(err_msg)
            raise RuntimeError(err_msg)
    except subprocess.CalledProcessError:
        err_msg = f"Unable to cancel job {job_id} from Slurm queue."
        _logger.exception(err_msg)
        raise
