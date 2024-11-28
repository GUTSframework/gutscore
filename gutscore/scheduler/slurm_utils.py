"""A set of SLURM utilities for gutscore."""
from __future__ import annotations
import logging
import os
import shlex
import subprocess
from typing import Any

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
        if partition not in self._all_part_nnodes:
            err_msg = f"Partition {partition} unknown!"
            raise ValueError(err_msg)

        # Assume the default partition hereafter
        if not partition:
            partition = self._default_partition

        if not partition and not self._default_partition:
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
