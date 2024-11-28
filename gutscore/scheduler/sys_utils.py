"""A set of utilities to get platform information."""
from __future__ import annotations
import getpass
import sys
import psutil


def is_mac_os() -> bool:
    """Indicates MacOS platform."""
    system = sys.platform.lower()
    return system.startswith("dar")

def is_windows_os() -> bool:
    """Indicates Windows platform."""
    system = sys.platform.lower()
    return system.startswith("win")

def is_linux_os() -> bool:
    """Indicates Linux platform."""
    system = sys.platform.lower()
    return system.startswith("lin")

def get_cpu_count() -> int | None:
    """Get the number of CPU on the system."""
    return psutil.cpu_count(logical=False)

def get_username() -> str:
    """Get the hostname."""
    return getpass.getuser()
