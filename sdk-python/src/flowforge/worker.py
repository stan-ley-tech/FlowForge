import logging
import os
import socket
import threading
import time
import uuid

from .client import Client, Task
from .errors import LeaseLost
from .workflow import Workflow

logger = logging.getLogger("flowforge.worker")


class Context:
    """Passed to every step function. `input` is the run's original
    input; `context` is every prior step's result, keyed by step name -
    the accumulated state a step can build on."""

    def __init__(self, task: Task):
        self.run_id = task.run_id
        self.step_name = task.step_name
        self.is_compensation = task.is_compensation
        self.compensation_of = task.compensation_of
        self.attempt = task.attempt
        self.max_attempts = task.max_attempts
        self.idempotency_key = task.idempotency_key
        self.input = task.input
        self.context = task.context

    def get(self, step_name: str, default=None):
        return self.context.get(step_name, default)


class Worker:
    """Polls the engine for work belonging to one workflow and executes
    it with the handlers registered on that Workflow object. A step
    function just returns a JSON-serializable result on success, or
    raises to signal failure - retry policy and compensation are the
    engine's job, not the worker's.
    """

    def __init__(
        self,
        client: Client,
        workflow: Workflow,
        worker_id: str = None,
        poll_interval: float = 1.0,
        heartbeat_interval: float = 10.0,
    ):
        self.client = client
        self.workflow = workflow
        self.worker_id = worker_id or self._default_worker_id()
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._stop = threading.Event()

    @staticmethod
    def _default_worker_id() -> str:
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    def stop(self):
        self._stop.set()

    def run(self):
        """Blocking poll loop. Runs until stop() is called or the
        process receives a KeyboardInterrupt."""
        logger.info("worker %s starting for workflow %s", self.worker_id, self.workflow.name)
        try:
            while not self._stop.is_set():
                task = self.client.poll_task(self.workflow.name, self.worker_id)
                if task is None:
                    self._stop.wait(self.poll_interval)
                    continue
                self._execute(task)
        except KeyboardInterrupt:
            logger.info("worker %s stopping", self.worker_id)

    def _execute(self, task: Task):
        spec = self.workflow.handler_for(task.step_name, task.is_compensation)
        if spec is None:
            logger.error("no handler registered for step %s (compensation=%s)", task.step_name, task.is_compensation)
            self._safe_fail(task, f"no handler registered for step {task.step_name!r}")
            return

        stop_heartbeat = threading.Event()
        hb_thread = threading.Thread(
            target=self._heartbeat_loop, args=(task, stop_heartbeat), daemon=True
        )
        hb_thread.start()

        try:
            result = spec.fn(Context(task))
        except Exception as exc:
            stop_heartbeat.set()
            hb_thread.join()
            logger.warning("step %s failed on attempt %d: %s", task.step_name, task.attempt, exc)
            self._safe_fail(task, str(exc))
            return

        stop_heartbeat.set()
        hb_thread.join()
        try:
            self.client.complete_task(task.step_id, task.lease_token, result if result is not None else {})
        except LeaseLost:
            logger.warning("step %s completed but its lease was already reclaimed", task.step_name)

    def _safe_fail(self, task: Task, reason: str):
        try:
            self.client.fail_task(task.step_id, task.lease_token, reason)
        except LeaseLost:
            pass

    def _heartbeat_loop(self, task: Task, stop_event: threading.Event):
        while not stop_event.wait(self.heartbeat_interval):
            try:
                self.client.heartbeat(task.step_id, task.lease_token)
            except LeaseLost:
                return
