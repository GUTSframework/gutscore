import pytest
from scheduler.slurm_utils import is_slurm_avail
from scheduler.sys_utils import is_linux_os
from scheduler.sys_utils import is_mac_os
from scheduler.sys_utils import is_windows_os


@pytest.fixture(scope="session")
def on_macos() -> None:
    """Fixture to check if tests running on MacOS."""
    if not is_mac_os():
        pytest.skip("Only runs on MacOS.")

@pytest.fixture(scope="session")
def on_windows() -> None:
    """Fixture to check if tests running on Windows."""
    if not is_windows_os():
        pytest.skip("Only runs on Windows.")

@pytest.fixture(scope="session")
def on_linux() -> None:
    """Fixture to check if tests running on Linux."""
    if not is_linux_os():
        pytest.skip("Only runs on Linux.")

@pytest.fixture(scope="session")
def slurm_available() -> None:
    """Fixture to check SLURM availability."""
    if not is_slurm_avail():
        pytest.skip("SLURM is not available on this system. Skipping SLURM-dependent tests.")

@pytest.fixture(scope="session")
def slurm_not_available() -> None:
    """Fixture to check SLURM unavailability."""
    if is_slurm_avail():
        pytest.skip("SLURM is available on this system. Skipping SLURM-missing tests.")
