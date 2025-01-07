"""A set of functions making up the workers."""
import logging
import time
from scheduler.event import Event
from scheduler.event import event_actions_dict
from scheduler.queue import Queue

_logger = logging.getLogger(__name__)

# Base worker function that will run in a separate process
def worker_function(queue : Queue | None,
                    wgroup_id : int,
                    worker_id : int,
                    max_tasks : int,
                    runtime : int) -> None:
    """Worker function that will run be executed in a separate process.

    Args:
        queue : A Queue with which the wgroup will interact
        wgroup_id : The id of the wgroup
        worker_id : The id of the worker
        max_tasks : The maximum number of tasks to be processed
        runtime : The maximum runtime of the worker
    """
    # Start the timer to trigger runtime termination
    time_start = time.time()

    # Set a tuple uniquely defining the worker
    wid = (wgroup_id, worker_id)

    # Worker might be queue-less for testing purposes
    if queue:
        # Register worker in queue
        queue.register_worker(wid)

    # TODO: add a hook function to initialize the worker data/ojects
    while True:
        # Check for function runtime termination
        if time.time() - time_start > runtime:
            warn_msg = f"Worker {wid} is stopping due to runtime exceeded {runtime} seconds."
            _logger.warning(warn_msg)
            if queue:
                queue.unregister_worker(wid)
            break

        # Worker might be queue-less for testing purposes
        if queue:
            # Check the completed task count
            if queue.get_completed_tasks() >= max_tasks:
                warn_msg = f"Worker {wid} is stopping as {max_tasks} tasks have been completed."
                _logger.warning(warn_msg)
                queue.unregister_worker(wid)
                break

            # Check for events in the queue
            event_data = queue.fetch_event()
            if event_data:
                _, _, event = event_data
                process_event(event, wid, queue)

            # Check for tasks in the queue
            task_data = queue.fetch_task()
            if task_data:
                task_id, task_uuid, task = task_data
                queue.update_worker_status(wid, f"in_progress-{task_id}")
                info_msg = f"Worker {wid} picked up task {task_id}"
                _logger.info(info_msg)

                # Execute the task using the registered functions
                task.execute()

                # Mark task as done
                queue.mark_task_done(task_uuid)
                info_msg = f"Worker {worker_id} completed task {task_id}"
                _logger.info(info_msg)

                # Atomically increment the completed task counter
                _ = queue.increment_completed_tasks()
                queue.update_worker_status(wid, "waiting")
            else:
                # If no tasks, wait for a while before trying again
                time.sleep(0.1)

        else:
            # If no queue, wait for a while before trying again
            time.sleep(0.1)

def process_event(event : Event,
                  wid : tuple[int,int],
                  queue : Queue) -> None:
    """Process the event action.

    Args:
        event: serialized event dictionary
        wid: the worker ID
        queue: the Queue from which the event was caught
    """
    wid_str = f"{wid[0]}-{wid[1]}"
    if event.target not in {wid_str, "all"}:
        return
    if event.action in event_actions_dict:
        event_actions_dict[event.action](wid, queue)
