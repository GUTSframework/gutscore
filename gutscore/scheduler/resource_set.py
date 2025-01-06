"""The resource_set classes for the gutscore."""
from __future__ import annotations
import json
import logging
import subprocess
from abc import ABCMeta
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
import toml
from scheduler.slurm_utils import make_job_script_wgroup
from scheduler.slurm_utils import submit_slurm_job
from scheduler.slurm_utils import time_to_s

if TYPE_CHECKING:
    from multiprocessing import Process

_logger = logging.getLogger(__name__)

class ResourceSetBaseclass(metaclass=ABCMeta):
    """A resource set base class for gutscore scheduler.

    A resource set base class, defining the API
    expected by the resource_manager class.

    The base class is responsible for the public initialization,
    while concrete implementation relies on `_init_rs` to set
    specific attributes.

    Attributes:
        _wgroup_id : The id of the workergroup this resource set is attached to.
        _nworkers : The number of workers in the resource set
        _runtime : The lifespan of the resource set
        _workers : The actual worker's Processes
        _worker_pids : Convenience list of worker's PID
    """
    def __init__(self,
                 wgroup_id : int,
                 res_config : dict[Any,Any] | None = None,
                 json_str : str | None = None) -> None:
        """Initialize the resource set.

        It can be initialized from a dictionary or from a
        previously serialized version of the set.

        Args:
            wgroup_id : The id of the group with which this set is associated
            res_config : A dictionary describing the resource set
            json_str : A json string describing the resource set
        """
        if not (res_config or json_str):
            err_msg = "Resource set must be initialized with either a dictionary or a json string"
            raise RuntimeError(err_msg)
        self._wgroup_id : int = wgroup_id
        self._nworkers : int = 0

        # Concrete class need to override self._runtime
        self._runtime : int | None = None

        if res_config:
            self._nworkers = res_config.get("nworkers", 1)
        if json_str:
            json_dict = json.loads(json_str)
            self._nworkers = json_dict.get("nworkers", 1)
        self._workers : list[Process] = []
        self._worker_pids : list[int] = []
        self._init_rs(res_config = res_config, json_str = json_str)

    @abstractmethod
    def _init_rs(self,
                 res_config : dict[Any,Any] | None = None,
                 json_str : str | None = None) -> None:
        """Asbtract method initializing the resource set.

        Args:
            res_config : A dictionary describing the resource set, passed from `__init__`
            json_str : A json string describing the resource set, passed from `__init__`
        """

    @abstractmethod
    def request(self, config : dict[Any,Any]) -> None:
        """Request the resources of the set.

        A non-blocking call responsible of using subprocess to request
        the resources of the set from the system.

        Args:
            config : The gutscore top-level configuration parameters dictionary.
        """

    @abstractmethod
    def serialize(self) -> str:
        """Serialize the resource set.

        Returns:
            A json_string describing the acquire resources, to be stored in the queue
        """

    @abstractmethod
    def from_json(self, resource_set_json : str) -> ResourceSetBaseclass:
        """Deserialize the resource set.

        Args:
            resource_set_json : A json string version of the resource set

        Returns:
            An instance of the particular derived class object
        """

    @abstractmethod
    def release(self) -> None:
        """Release the resources of the set.

        Raises:
            RuntimeError : If something went wrong while releasing the resource
        """

    def get_nworkers(self) -> int:
        """Get the (theoretical) number of workers.

        Returns:
            The (theoretical) number of workers
        """
        return self._nworkers

    def get_nworkers_active(self) -> int:
        """Get the active number of workers.

        Returns:
            The active number of workers
        """
        return len(self._workers)

    def workers_initiated(self) -> bool:
        """Test if all workers are initiated.

        Returns:
            True if all workers are initiated
        """
        return len(self._workers) >= self._nworkers

    def append_worker_process(self, process : Process) -> None:
        """Add a worker process to the list.

        Args:
            process : a worker process
        """
        self._workers.append(process)
        try:
            self._workers[-1].start()
        except Exception:
            err_msg = f"{self._wgroup_id} failed to start worker process"
            _logger.exception(err_msg)
            raise
        pid = self._workers[-1].pid
        if pid:
            self._worker_pids.append(pid)
        else:
            err_msg = f"{self._wgroup_id} failed to get worker process pid"
            raise RuntimeError(err_msg)

    def worker_runtime(self) -> int:
        """Return the worker runtime.

        Returns:
            The worker runtime [s]
        """
        if not self._runtime:
            err_msg = "Runtime not set"
            raise RuntimeError(err_msg)
        return self._runtime

class ResourceSetLocal(ResourceSetBaseclass):
    """A local resource set class for gutscore scheduler.

    Manage the resource available locally in the session.
    This is the appropriate backend when working on a personal
    computer.

    Attributes:
        _runtime : A runtime limit in seconds
        _deamonize : Release the main process, effectively daemonizing the workers ?
    """
    def _init_rs(self,
                 res_config : dict[Any,Any] | None = None,
                 json_str : str | None = None) -> None:
        """Initialize the local resource set attributes.

        Args:
            res_config : A dictionary describing the resource set
            json_str : A json string describing the resource set
        """
        if res_config:
            self._runtime = res_config.get("runtime", 100)
            self._daemonize = res_config.get("daemonize", False)

        if json_str:
            json_dict = json.loads(json_str)
            self._runtime = json_dict.get("runtime", 100)
            self._daemonize = json_dict.get("daemonize", False)

    def request(self, config : dict[Any,Any]) -> None:
        """Request the resources of the set.

        Use subprocess to launch the run_workergroup CLI
        with proper arguments.

        Args:
            config : The GUTS top-level configuration parameters dictionary.

        Raises:
            RuntimeError : If it fails to create the child process
        """
        # Check resource set was provided a definition
        if not self._runtime:
            err_msg = "Runtime not set while requesting resources"
            raise RuntimeError(err_msg)

        # Process the configuration dict
        toml_file = f"input_WG{self._wgroup_id:05d}.toml"
        wgroup_config = dict(config)
        wgroup_config[f"WG{self._wgroup_id:05d}"] = {"NullField" : 0}
        with Path(toml_file).open("w") as f:
            toml.dump(wgroup_config, f)

        # Request command
        wgroup_cmd = ["run_workergroup"]
        wgroup_cmd.append("-i")
        wgroup_cmd.append(toml_file)
        wgroup_cmd.append("-wg")
        wgroup_cmd.append(f"{self._wgroup_id}")

        # stdout/stderr, handles passed to the child process
        stdout_name = f"stdout_{self._wgroup_id:05d}.txt"
        stderr_name = f"stderr_{self._wgroup_id:05d}.txt"
        with Path(stdout_name).open("wb") as outf, \
             Path(stderr_name).open("wb") as errf:
            try:
                subprocess.Popen(wgroup_cmd,
                                 stdout = outf,
                                 stderr = errf,
                                 start_new_session=True)
            except Exception:
                err_msg = f"Unable to request resource using subprocess.Popen({wgroup_cmd})"
                _logger.exception(err_msg)
                raise

    def serialize(self) -> str:
        """Serialize the local resource set.

        Returns:
            A json string version of the resource set
        """
        return json.dumps({
            "type": "local",
            "wgroup_id": self._wgroup_id,
            "nworkers": self._nworkers,
            "runtime": self._runtime,
            "daemonize": self._daemonize,
        })

    def from_json(self, resource_set_json : str) -> ResourceSetLocal:
        """Deserialize the local resource set.

        Args:
            resource_set_json : A json string version of the resource set

        Returns:
            The deserialized resource set
        """
        res_dict = json.loads(resource_set_json)
        return ResourceSetLocal(res_dict["wgroup_id"], json_str = resource_set_json)

    def release(self) -> None:
        """Release the resources."""

class ResourceSetSlurm(ResourceSetBaseclass):
    """A slurm resource set class for the gutscore scheduler..

    Manage the resource available on an HPC cluster with the Slurm
    job scheduler.

    Attributes:
    _slurm_job_id : The Slurm job ID, obtained after the job is submitted to the scheduler
    _slurm_job_script : The Slurm batch script, assembled from resource set description
    """
    def _init_rs(self,
                 res_config : dict[Any,Any] | None = None,
                 json_str : str | None = None) -> None:
        """Initialize the local resource set attributes.

        Args:
            res_config : A dictionary describing the resource set
            json_str : A json string describing the resource set
        """
        if res_config:
            self._slurm_job_id = None
            self._slurm_job_script = self._build_job_script(self._wgroup_id, res_config)
            self._runtime = time_to_s(res_config.get("runtime", None))
        if json_str:
            json_dict = json.loads(json_str)
            self._slurm_job_id = json_dict.get("job_id")
            self._slurm_job_script = json_dict.get("job_script")
            self._runtime = time_to_s(str(json_dict.get("runtime")))

    def request(self, config : dict[Any,Any]) -> None:
        """Request the resources of the set.

        Args:
            config : The scheduler top-level configuration parameters dictionary.

        Raises:
            RuntimeError : If it fails to submit the batch script
        """
        # Check resource set was provided a definition
        if not self._runtime:
            err_msg = "Runtime not set while requesting resources"
            raise RuntimeError(err_msg)

        # Process the configuration dict
        toml_file = f"input_WG{self._wgroup_id:05d}.toml"
        wgroup_config = dict(config)
        wgroup_config[f"WG{self._wgroup_id:05d}"] = {"NullField" : 0}
        with Path(toml_file).open("w") as f:
            toml.dump(wgroup_config, f)

        try:
            job_id = submit_slurm_job(self._wgroup_id,
                                      self._slurm_job_script)
            self._slurm_job_id = job_id
        except Exception:
            err_msg = f"Unable to submit batch script to slurm for wgroup {self._wgroup_id}"
            _logger.exception(err_msg)
            raise


    def serialize(self) -> str:
        """Serialize the slurm resource set.

        Returns:
            A json string version of the resource set
        """
        return json.dumps({
            "type": "slurm",
            "wgroup_id": self._wgroup_id,
            "nworkers": self._nworkers,
            "runtime": self._runtime,
            "job_id": self._slurm_job_id,
            "job_script": self._slurm_job_script,
        })

    def from_json(self, resource_set_json : str) -> ResourceSetSlurm:
        """Deserialize the slurm resource set.

        Args:
            resource_set_json : A json string version of the resource set

        Returns:
            The deserialized resource set
        """
        res_dict = json.loads(resource_set_json)
        return ResourceSetSlurm(res_dict["wgroup_id"], json_str = resource_set_json)

    def _build_job_script(self,
                          wgroup_id : int,
                          res_config : dict[Any,Any]) -> list[str]:
        """Build the job script from config.

        Args:
            wgroup_id : The id of the worker group
            res_config : A dictionary containing the entries
                         needed for assembling a Slurm job script

        Returns:
            The content of the job script as a list of strings
        """
        return make_job_script_wgroup(wgroup_id, res_config)

    def release(self) -> None:
        """Release the slurm resource."""
