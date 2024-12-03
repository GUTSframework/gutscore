"""A resource manager class for the scheduler."""
from __future__ import annotations
import json
import logging
from typing import Any
from scheduler.resource_set import ResourceSetBaseclass
from scheduler.resource_set import ResourceSetLocal
from scheduler.resource_set import ResourceSetSlurm
from scheduler.slurm_utils import SlurmCluster
from scheduler.sys_utils import get_cpu_count

_logger = logging.getLogger(__name__)

class ResourceManager:
    """A resource manager class for gutscore scheduler.

    The resource manager interface between the workergroups
    and the compute resources of the system.

    Attributes:
        _nwgroups : The number of worker groups the manager has to handle
        _backend : The type of compute resource available in the backend (local, slurm, ...)
        _configs : The GUTS full parmeters dictionary, augmented if necessary
    """
    def __init__(self,
                 configs : dict[Any,Any]) -> None:
        """Initialize the resource manager.

        Args:
            configs : The GUTS full parmeters dictionary

        Raises:
            ValueError : If a configuration parameters has a wrong value
            RuntimeError : If if was not possible to access the resource backend
        """
        self._nwgroups : int = configs.get("resource",{}).get("nwgroups", 1)
        self._backend : str = configs.get("resource",{}).get("backend", "local")
        self._configs : dict[Any,Any] = configs

        if self._backend == "local":
            # Keep track of local cpu resources available
            self._max_cpus : int = get_cpu_count()
            self._used_cpus : int = 0

        elif self._backend == "slurm":
            # Gather information on the system using SlurmCluster
            try:
                self._cluster = SlurmCluster()
            except Exception:
                _logger.exception("Error while instanciating a SlurmCluster")
                raise

    def get_nwgroups(self) -> int:
        """Get the number of workergroups.

        Returns:
            The number of workergroups
        """
        return self._nwgroups

    def get_resources(self,
                      res_configs : dict[Any,Any],
                      wgroup_id : int) -> ResourceSetBaseclass:
        """Get the resources from the manager.

        Args:
            res_configs : A dictionary describing the resource set
            wgroup_id : The id of the workergroup for which the resources are acquired

        Returns:
            A resource set of the specified concrete type

        Raises:
            ValueError : If the resource specification has wrong parameters
            RuntimeError : If something went wrong while trying to acquire the resources
        """
        if self._backend == "local":
            try:
                res_l = ResourceSetLocal(wgroup_id, res_config = res_configs)
            except:
                err_msg = f"Resource manager caught a wrong parameter with wgroup {wgroup_id} resource set"
                _logger.exception(err_msg)
                raise
            self._used_cpus += res_l.get_nworkers()
            if self._used_cpus > self._max_cpus:
                err_msg = f"Maximum number of CPUs reached: {self._max_cpus}"
                raise RuntimeError(err_msg)
            try:
                res_l.request(self._configs)
            except RuntimeError:
                err_msg = f"Resource manager failed to request local resources for wgroup {wgroup_id}"
                _logger.exception(err_msg)
                raise
            return res_l

        if self._backend == "slurm":
            try:
                updated_configs = self._cluster.process_res_config(res_configs)
            except:
                err_msg = f"Resource manager caught a wrong parameter with wgroup {wgroup_id} resource set"
                _logger.exception(err_msg)
                raise
            res_s = ResourceSetSlurm(wgroup_id, res_config = updated_configs)
            try:
                res_s.request(self._configs)
            except RuntimeError:
                err_msg = f"Resource manager failed to request slurm resources for wgroup {wgroup_id}"
                _logger.exception(err_msg)
                raise
            return res_s

        err_msg = f"Unknown backend '{self._backend}'"
        raise ValueError(err_msg)

    def instanciate_resource(self,
                             res_json : str) -> ResourceSetBaseclass:
        """Instanciate a resource set from a json string.

        Args:
            res_json : The json string produced by serialization of a resource set

        Raises:
            ValueError : When mixing backends or providing erroneous wgroup ids
        """
        backend = json.loads(res_json)["type"]
        if backend != self._backend:
            err_msg = "Resource manager mix-up: instanciating resource from json with different backend"
            raise ValueError(err_msg)

        wgroup_id = json.loads(res_json)["wgroup_id"]
        if self._backend == "local":
            return ResourceSetLocal(wgroup_id, json_str = res_json)

        if self._backend == "slurm":
            return ResourceSetSlurm(wgroup_id, json_str = res_json)

        err_msg = f"Unknown backend '{self._backend}'"
        raise ValueError(err_msg)
