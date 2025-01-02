"""A front end for gutscore scheduler."""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import TYPE_CHECKING
import toml
from scheduler.queue import Queue
from scheduler.resource_manager import ResourceManager
from scheduler.workergroup import WorkerGroup

if TYPE_CHECKING:
    from scheduler.task import Task


def parse_cl_args(a_args : list[str] | None = None) -> argparse.Namespace :
    """Parse provided list or default CL argv.

    Args:
        a_args: optional list of options
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="scheduler input .toml file", default="input.toml")
    parser.add_argument("-wg", "--wgroup", help="workergroup number")
    return parser.parse_args(a_args) if a_args is not False else parser.parse_args()

class Scheduler:
    """A scheduler class for gutscore.

    The scheduler is the top-level, user-facing end of gutscore compute
    system, which includes a disk-based queue, a resources manager
    and a set of worker groups, each of which containing multiple
    workers with a set of resources.

    The scheduler is designed to not be persistent, but rather
    respawned at will by the user or the individual worker group
    themselves.

    Attributes:
        _params : The configuration parameters dictionary
        _name : The scheduler name
        _queue_file : The name of the guts_queue file associated with the scheduler
        _queue : A Queue object with which the scheduler interacts
        _manager : A ResourceManager to dispatch resources to worker groups
        _wgroups : A list of WorkerGroup
        _nwgroups : The number of WorkerGroups the scheduler manage
    """
    def __init__(self,
                 a_args: list[str] | None = None) -> None:
        """Initialize scheduler internals.

        Args:
            a_args: optional list of options

        Raises:
            ValueError : If the input file does not exist, wgroup number is not valid
        """
        # Read input parameters
        input_file = vars(parse_cl_args(a_args=a_args))["input"]
        if (not Path(input_file).exists()):
            err_msg = f"Could not find the scheduler {input_file} input file !"
            raise ValueError(err_msg)
        with Path(input_file).open("r") as f:
            self._params = toml.load(f)

        # Add the input file name to the dict for future reference
        self._params["input_toml"] = input_file


        # Scheduler metadata
        self._name = self._params.get("case",{}).get("name","guts_run")
        self._queue_file = self._params.get("case",{}).get("queue_file",f"{self._name}_queue.db")
        self._queue = Queue(self._queue_file)

        # Worker groups & compute resources
        self._manager = ResourceManager(self._params)
        self._wgroups : list[WorkerGroup] = []

        # Minimalist internal initiation
        self._nwgroups = self._manager.get_nwgroups()

        # TODO: enable map wgroups to different resource configs
        res_config = self._params.get("resource",{}).get("config",{})
        for i in range(self._nwgroups):
            self._wgroups.append(WorkerGroup(i,
                                             self._params,
                                             res_config,
                                             queue=self._queue))

        # Parse workergroup number if provided
        wgroup_id_str = vars(parse_cl_args(a_args=a_args))["wgroup"]
        self._wgroup_id_respawn = -1
        if wgroup_id_str:
            self._wgroup_id_respawn = int(wgroup_id_str)

    def start(self) -> None:
        """Start the scheduler.

        This a non-blocking call, where each workergroup is initiated
        and launched seperately depending on the resource backend type.
        """
        # Initiate the worker groups by requesting resource from the manager
        for wgroup in self._wgroups:
            wgroup.request_resources(self._manager)

    def run_wgroup(self) -> None:
        """Run a given workergroup.

        Raises :
            RuntimeError : If there is no workergroup targeted to run
        """
        if (self._wgroup_id_respawn >= self._nwgroups or
           self._wgroup_id_respawn < 0 ):
            err_msg = f"Unable to run Workergroup {self._wgroup_id_respawn} does not exist !"
            raise ValueError(err_msg)
        target_wgroup = self._wgroups[self._wgroup_id_respawn]
        target_wgroup.launch(self._manager)

    def wgroup_id_respawn(self) -> int:
        """Access the target respawn wgroup id.

        Returns :
            The id of the wgroup to respawn
        """
        return self._wgroup_id_respawn

    def check(self) -> None:
        """Check the scheduler queue and workergroups status."""

    def restore(self) -> None:
        """Restore the workergroups if needed."""

    def get_queue(self) -> Queue:
        """Return the scheduler queue.

        Returns :
            The scheduler queue
        """
        return self._queue

    def add_task(self, task : Task) -> None:
        """Add a new task to the queue.

        Args:
            task : The task to add to the queue
        """
        self._queue.add_task(task)

    def name(self) -> str:
        """Return the case name."""
        return self._name

    def cleanup(self) -> None:
        """Clean scheduler internals."""
        self._queue.delete()
