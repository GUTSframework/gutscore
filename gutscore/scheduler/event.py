"""A class to represent scheduler events."""
from __future__ import annotations
import json
from dataclasses import dataclass


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
