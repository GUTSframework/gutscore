"""A worker group class for the gutscore scheduler."""
from __future__ import annotations
import logging
from multiprocessing import Process
from typing import TYPE_CHECKING
from typing import Any
from scheduler.worker import worker_function

if TYPE_CHECKING:
    from scheduler.queue import Queue
    from scheduler.resource_manager import ResourceManager

_logger = logging.getLogger(__name__)

class WorkerGroup:
    """A workergroup class gathering some workers with a given set of resources.

    This is a volatile class, providing granularity in
    the allocation of the compute resources to workers
    within the gutscore scheduler.

    Attributes :
        _config : A dictionary of configuration parameters
        _wgroup_id : The group id number
        _resource_config : A dictionary describing the compute resources associated to the group
        _resources_set : A resource set object gathering resources functionalities
        _queue : The scheduler queue this group interacts with
    """
    def __init__(self,
                 wgroup_id : int,
                 config : dict[Any,Any],
                 resource_config : dict[Any,Any],
                 queue : Queue | None = None) -> None:
        """Initialize the worker group.

        Args :
            wgroup_id : The group id number
            config : The scheduler top-level dictionary of configuration parameters
            resource_config : A dictionary describing the compute resources associated to the group
            queue : The scheduler queue this group interacts with
        """
        # Metadata
        self._config = config
        self._wgroup_id = wgroup_id

        # Resource configuration
        self._resource_config = resource_config
        self._resources_set = None

        # Set queue if present and register wgroup
        self._queue = None
        if queue:
            self._queue = queue
            self._queue.register_worker_group(wgroup_id)

    def id(self) -> int:
        """Get the id of the worker group.

        Returns :
            The id of the worker group
        """
        return self._wgroup_id

    def get_queue(self) -> Queue | None:
        """Get the queue of the worker group.

        Returns :
            The queue of the worker group
        """
        return self._queue

    def attach_queue(self, queue : Queue) -> None:
        """Attach a queue to the worker group.

        Args :
            queue : A Queue with which the wgroup will interact

        Raises :
            RuntimeError : If the workergroup already has a queue attached
        """
        if not self._queue:
            self._queue = queue
            self._queue.register_worker_group(self._wgroup_id)
        else:
            err_msg = "Cannot overwrite queue in worker group"
            raise RuntimeError(err_msg)

    def request_resources(self,
                          manager : ResourceManager) -> None:
        """Request resources for the worker group.

        This function request resources from the manager, effectively
        relinquishing the control of the program to a subprocess in
        order to harmonize workflow accross various resource backend.

        Args :
            manager : A ResourceManager from which to get the resources

        Raises:
            RuntimeError : If the worker group is not associated with a queue
        """
        # Check that the worker group is associated with a queue
        if not self._queue:
            err_msg = f"Worker group {self._wgroup_id} cannot request resources without a queue"
            raise RuntimeError(err_msg)

        # Get the resources from the manager
        self._resource_set = manager.get_resources(self._resource_config,
                                                   self._wgroup_id)

        # Update the queue with worker group request resources info
        self._queue.update_worker_group_resources(self._wgroup_id,
                                                  self._resource_set.serialize())

    def launch(self,
               manager : ResourceManager) -> None:
        """Launch the workers in the group.

        This is the entry-point of the workergroup after a request_resources call.
        Resources are looked up from the queue, checked in the current
        environment and worker are fired up.

        Args :
            manager : A ResourceManager to handle resource set and checks

        Raises:
            RuntimeError : If the worker group is not associated with a queue
        """
        # Check that the worker group is associated with a queue
        if not self._queue:
            err_msg = f"Worker group {self._wgroup_id} cannot request resources without a queue"
            raise RuntimeError(err_msg)

        # Query the queue for the resources set
        res_json = self._queue.get_worker_group_resource(self._wgroup_id)
        if not res_json:
            err_msg = f"Unable to get resource set for wgroup {self._wgroup_id} from queue !"
            raise RuntimeError(err_msg)
        self._resource_set = manager.instanciate_resource(res_json)

        w_runtime = self._resource_set.worker_runtime()
        wid = 0
        while not self._resource_set.workers_initiated():
            # Each worker runs the `worker_function` in a separate process
            self._resource_set.append_worker_process(Process(target=worker_function,
                                                             args=(self._queue,
                                                                   self._wgroup_id,
                                                                   wid,
                                                                   100,
                                                                   w_runtime)))
            wid = wid + 1
