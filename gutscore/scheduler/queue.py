"""A queue class for the gutscore scheduler."""
from __future__ import annotations
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Literal
from scheduler.event import Event
from scheduler.task import Task

_logger = logging.getLogger(__name__)

class Queue:
    """A disk-based SQL queue for the gutscore scheduler.

    The schduler queue is a lightweight class interfacing with a
    disk-based SQL queue. It provides the scheduler
    components (workers, workergroup, resource manager, tasks, ...)
    an atomic process to interact with the flow of GUTS algorithms.

    Attributes:
        _db_name : The queue file name
    """
    def __init__(self,
                 db_name : str = "guts_queue.db"):
        """Initialize the queue.

        Args:
            db_name : The queue file name
        """
        self._db_name = db_name
        self._init_db()

    def file_name(self) -> str:
        """Return the queue file name.

        Returns:
            The queue file name
        """
        return self._db_name

    def _init_db(self) -> None:
        """Initialize the content of the queue file.

        Raises:
            RuntimeError : If a connection to the DB could not be acquired
        """
        conn = self._connect()
        cursor = conn.cursor()

        # Create tasks table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid GUID  NOT NULL DEFAULT NONE,
            dep GUID NOT NULL DEFAULT NONE,
            task_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        """)

        # Create a table to track completed tasks
        # TODO: might be useless
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS task_counter (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            completed_tasks INTEGER DEFAULT 0
        )
        """)

        # Ensure the counter row is initialized (there will be only one row with id=1)
        cursor.execute("INSERT OR IGNORE INTO task_counter (id, completed_tasks) VALUES (1, 0)")

        # Create an events queue
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acc_count INTEGER DEFAULT 0,
            event_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        """)

        # Create a worker register
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER NOT NULL,
            gid INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting'
        )
        """)

        # Create a worker group register
        # status
        # 'pending'
        #   Upon requesting resources for a worker group.
        # 'active'
        #   When worker group has initiated his workers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS worker_groups (
            id INTEGER NOT NULL,
            resource_set_json TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        """)

        conn.commit()
        conn.close()

    def _connect(self,
                 isolation_level : Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] = "DEFERRED") -> sqlite3.Connection:
        """Create a new SQLite connection for each process.

        Args:
            isolation_level : The isolation level of the connection
        """
        # Append uuid to SQL supported types
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter("GUID", lambda b: uuid.UUID(bytes_le=b))
        try:
            return sqlite3.connect(self._db_name,
                                   isolation_level = isolation_level)
        except Exception:
            err_msg = f"Unable to connect to {self._db_name}"
            _logger.exception(err_msg)
            raise

    def _exec_sql_getone(self,
                         sql_cmd : str,
                         args : tuple[int | str | float, ...] | None = None) -> int:
        """Execute an SQL command and return the first result.

        Args:
            sql_cmd : The SQL command litteral to execute
            args : The arguments to pass to the SQL command

        Returns:
            The first result of the SQL command
        """
        conn = self._connect()
        cursor = conn.cursor()
        res = cursor.execute(sql_cmd).fetchone()[0] if args is None \
            else cursor.execute(sql_cmd, args).fetchone()[0]
        conn.commit()
        conn.close()
        return res

    def add_task(self,
                 task : Task,
                 deps : uuid.UUID | None = None) -> uuid.UUID:
        """Add a new task to the queue.

        Args:
            task : The task to add to the queue
            deps : The UUID of the task that this task depends on

        Returns:
            The UUID of the added task
        """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter("GUID", lambda b: uuid.UUID(bytes_le=b))
        conn = self._connect()
        cursor = conn.cursor()
        t_uuid = uuid.uuid4()
        if deps:
            cursor.execute("SELECT id FROM tasks WHERE uuid = ?", (deps,))
            data = cursor.fetchone()
            if data is None:
                err_msg = "Can't add dependencies to a non-existing task"
                raise RuntimeError(err_msg)
            cursor.execute("INSERT INTO tasks (uuid, dep, task_json, status) VALUES (?, ?, ?, ?)",
                           (t_uuid, deps, task.to_json(), "pending"))
        else:
            cursor.execute("INSERT INTO tasks (uuid, task_json, status) VALUES (?, ?, ?)",
                           (t_uuid, task.to_json(), "pending"))
        conn.commit()
        conn.close()
        return t_uuid

    def fetch_task(self) -> tuple[int, uuid.UUID, Task] | None:
        """Fetch the next pending task and mark it as 'in_progress'.

        A BEGIN EXCLUSIVE is nessesary to avoid race conditions from
        other worker between the select and update.

        Returns:
            A tuple of (task_id, task_uuid, task) or None
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
        cursor.execute("SELECT id, uuid, dep, task_json FROM tasks WHERE status = ? ORDER BY id LIMIT 1", ("pending",))
        task_data = cursor.fetchone()

        if task_data:
            task_id, task_uuid, task_dep, task_json = task_data
            cursor.execute("UPDATE tasks SET status = ? WHERE id = ?", ("in_progress", task_id))
            conn.commit()
            conn.close()
            return task_id, task_uuid, Task.from_json(task_json)
        conn.close()

        return None

    def mark_task_done(self,
                       task_uuid : uuid.UUID) -> None:
        """Mark the task as done.

        Args:
            task_uuid : The UUID of the task
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET status = ? WHERE uuid = ?", ("done", task_uuid))
        conn.commit()
        conn.close()

    def increment_completed_tasks(self) -> int:
        """Atomically increment the completed tasks counter and fetch its new value.

        Returns:
            The new value of the completed tasks counter
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE task_counter SET completed_tasks = completed_tasks + 1 WHERE id = 1")
        cursor.execute("SELECT completed_tasks FROM task_counter WHERE id = 1")
        completed_tasks = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return completed_tasks

    def get_completed_tasks(self) -> int:
        """Retrieve the current value of the completed tasks counter.

        Returns:
            The current value of the completed tasks counter
        """
        return self._exec_sql_getone("SELECT completed_tasks FROM task_counter WHERE id = 1")

    def get_running_tasks_count(self) -> int:
        """Return the number of tasks marked in-progress.

        Returns:
            The number of tasks marked in-progress
        """
        return self._exec_sql_getone('SELECT COUNT() FROM tasks WHERE status = "in_progress"')

    def get_remaining_tasks_count(self) -> int:
        """Return the number of tasks marked pending/in-progress.

        Returns:
            The number of tasks marked pending/in-progress
        """
        return self._exec_sql_getone('SELECT COUNT() FROM tasks WHERE status IN ("pending", "in_progress")')

    def get_tasks_count(self) -> int:
        """Return the total number of tasks in the queue.

        Returns:
            The total number of tasks
        """
        return self._exec_sql_getone("SELECT COUNT() FROM tasks")

    def add_event(self, event : Event) -> None:
        """Add a new event to the queue.

        Args:
            event : The event to add
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO events (event_json, status) VALUES (?, ?)",
                       (event.to_json(), "pending"))
        conn.commit()
        conn.close()

    def fetch_event(self) -> tuple[int, int, Event] | None:
        """Fetch the next pending event.

        Returns:
            A tuple of (event_id, acc_count, event)
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT id, acc_count, event_json, \
            status FROM events WHERE status = ? ORDER BY id LIMIT 1", ("pending",))
        event_data = cursor.fetchone()

        if event_data:
            event_id, acc_count, event_json, status = event_data
            acc_count = acc_count + 1
            cursor.execute("UPDATE events SET acc_count = ? WHERE id = ?", (acc_count, event_id))
            conn.commit()
            conn.close()
            return event_id, acc_count, Event.from_json(event_json)

        conn.close()
        return None

    def get_events_count(self) -> int:
        """Return the total number of events in the queue.

        Returns:
            The total number of events
        """
        return self._exec_sql_getone("SELECT COUNT() FROM events")


    def register_worker(self,
                        wid : tuple[int,int]) -> None:
        """Register a worker in the queue.

        Args:
            wid : The worker id, defined by group and worker
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO workers (gid, id) VALUES (?, ?)", (wid[0], wid[1]))
        conn.commit()
        conn.close()

    def unregister_worker(self,
                          wid : tuple[int,int]) -> None:
        """Unregister a worker from the queue.

        Args:
            wid : The worker id, defined by group and worker
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM workers WHERE gid = ? AND id = ?", (wid[0], wid[1]))
        conn.commit()
        conn.close()

    def update_worker_status(self,
                             wid : tuple[int,int], status : str) -> None:
        """Update the worker status in queue.

        Args:
            wid : The worker id, defined by group and worker
            status : The worker status
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE workers SET status = ? WHERE gid = ? AND id = ?", (status, wid[0], wid[1]))
        conn.commit()
        conn.close()

    def get_workers_count(self) -> int:
        """Return the number of workers.

        Returns:
            The number of workers
        """
        return self._exec_sql_getone("SELECT COUNT() FROM workers")

    def get_active_workers_count(self) -> int:
        """Return the number of active workers.

        Returns:
            The number of active workers
        """
        return self._exec_sql_getone('SELECT COUNT() FROM workers WHERE status = "working"')

    def delete(self,
               timeout : int = 60) -> None:
        """Delete the DB when all tasks and workers are done.

        Args:
            wait_for_done : Wait until all tasks and workers are done
            timeout : Timeout in seconds
        """
        # Initialize time for timeout
        time_st = time.time()

        # Trigger a kill all event
        self.add_event(Event(eid = 1, action ="worker-kill", target = "all"))

        # Wait until no more tasks/workers active or timeout
        while ((self.get_remaining_tasks_count() > 0
            or self.get_workers_count() > 0 )
            and time.time() - time_st < timeout):
            time.sleep(0.1)

        # Actually delete the DB
        Path(self._db_name).unlink()
