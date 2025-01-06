"""A class to represent scheduler events."""
from __future__ import annotations
import json
import logging
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.queue import Queue

_logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Event class, triggering specific worker behaviors.

    Attributes:
        eid : event id
        action : an action name
        target : an optional event target (wgroup, worker, ...)
    """
    eid : int
    action : str
    target : str | None = None

    def to_json(self) -> str:
        """Serialize the event to a JSON string for storage.

        Returns:
            A JSON string
        """
        return json.dumps({
            "id": self.eid,
            "action": self.action,
            "target": self.target,
        })

    @staticmethod
    def from_json(event_json : str) -> Event:
        """Deserialize an event from a JSON string.

        Args:
            event_json : a JSON string

        Returns:
            An event
        """
        event_dict = json.loads(event_json)
        return Event(event_dict["id"],
                     event_dict["action"],
                     event_dict["target"])

def stop_worker(wid : tuple[int,int],
                queue : Queue) -> None:
    """Action to stop a worker.

    Args:
        wid : the worker ID
        queue : the Queue from which the event was caught
    """
    queue.unregister_worker(wid)
    info_msg = f"Worker {wid} stopped."
    _logger.info(info_msg)
    sys.exit(0)

# Dictionary of event actions
event_actions_dict = {
        "worker-kill": stop_worker,
        }
