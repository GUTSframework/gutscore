"""The CLI functions for the gutscore scheduler."""
from scheduler.scheduler import Scheduler


def sched_workergroup() -> None:
    """Start a workergroup from a guts scheduler."""
    scheduler = Scheduler()
    scheduler.run_wgroup()
