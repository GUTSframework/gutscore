"""A set of SLURM utilities for gutscore."""
import logging
import os
import subprocess

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
