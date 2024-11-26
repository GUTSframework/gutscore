"""A plain class to represent GUTS tasks."""
from __future__ import annotations
import json
import time
from typing import Any, Optional

# A register of task functions
task_functions_reg = {}

def register_taskf(function_name : str):
    """Add a task function in the register.

    Args:
    function_name: str
        The name of the function
    """
    def decorator(function):
        task_functions_reg[function_name] = function
        return function
    return decorator

def unregister_taskf(function_name : str) -> None:
    """Remove a task function from the register.

    Args:
    function_name: str
        The name of the function to remove
    """
    if function_name in task_functions:
        del task_functions_reg[function_name]

@register_taskf('nap_test')
def nap_test(nap_duration : float = 1.0) -> None:
    """A test function used in testing the scheduler."""
    time.sleep(nap_duration)

# Task class to represent a callable function with arguments
class Task:
    """A class reprenting GUTS scheduler tasks.

    Tasks are units of work that workers can execute. They can
    be serialized, stored in the scheduler database and retrieved
    later for execution.

    The current implementation relies on an in-memory function register to map
    function names stored in the tasks to Python concrete functions.
    A disk-based function register might be better suited in this framework.

    Attributes:
    _function_name: str    
        The name of the function to call
    _args: dict[Any]
        The optional arguments dictionary to pass to the function
    """
    def __init__(self,
                 function_name : str,
                 args : Optional[dict[Any]] = {}) -> None:
        """Initialize the task.

        Args:
        function_name: str
            The name of the function to call
        args: dict[Any]
            The optional arguments dictionary to pass to the function
        """
        self._function_name = function_name  # String name of the function to call
        self._args = args

    def to_json(self) -> str:
        """Serialize the task to a JSON string for storage.

        Returns:
        str
            The JSON string of the task
        """
        return json.dumps({
            'function_name': self._function_name,
            'args': self._args
        })

    @staticmethod
    def from_json(task_json) -> task:
        """Deserialize a task from a JSON string.
        """
        task_dict = json.loads(task_json)
        return Task(task_dict['function_name'], task_dict['args'])

    def execute(self) -> None:
        """Execute the task by calling the corresponding function.

        Raises:
        ValueError
            If the function is not registered
        RuntimeError
            If the function execution fails
        """
        func = task_functions_reg.get(self._function_name)
        if func is None:
            raise ValueError(f"Function '{self.function_name}' is not registered.")
        
        try:
            func(**self._args)
        except Exception as e:
            print(e)
            raise RuntimeError(f"Error while executing task function: {self._function_name}")
