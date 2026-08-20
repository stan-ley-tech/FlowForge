import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

from .errors import FlowForgeError, LeaseLost, RunAlreadyTerminal, RunNotFound, WorkflowNotFound
from .workflow import Workflow


@dataclass
class Task:
    step_id: str
    run_id: str
    step_name: str
    is_compensation: bool
    compensation_of: str
    attempt: int
    max_attempts: int
    timeout_seconds: int
    idempotency_key: str
    lease_token: str
    input: Any
    context: dict


def _task_from_json(body: dict) -> Task:
    return Task(
        step_id=body["step_id"],
        run_id=body["run_id"],
        step_name=body["step_name"],
        is_compensation=body.get("is_compensation", False),
        compensation_of=body.get("compensation_of", ""),
        attempt=body["attempt"],
        max_attempts=body["max_attempts"],
        timeout_seconds=body["timeout_seconds"],
        idempotency_key=body["idempotency_key"],
        lease_token=body["lease_token"],
        input=body.get("input"),
        context=body.get("context") or {},
    )


class Client:
    """Thin HTTP client for the FlowForge engine's REST API. Uses only
    the standard library so the SDK has no third-party dependencies."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: Optional[dict] = None):
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status == 204:
                    return None
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            message = raw.decode("utf-8", "replace") if raw else exc.reason
            try:
                message = json.loads(raw).get("error", message)
            except (json.JSONDecodeError, AttributeError):
                pass
            raise self._error_for(exc.code, message) from None

    @staticmethod
    def _error_for(status_code: int, message: str) -> FlowForgeError:
        lowered = message.lower()
        if status_code == 404:
            if "workflow" in lowered:
                return WorkflowNotFound(message, status_code)
            if "run" in lowered:
                return RunNotFound(message, status_code)
        if status_code == 409:
            if "terminal" in lowered:
                return RunAlreadyTerminal(message, status_code)
            return LeaseLost(message, status_code)
        return FlowForgeError(message, status_code)

    def register_workflow(self, workflow: Workflow) -> dict:
        return self._request("POST", "/v1/workflows", workflow.to_definition())

    def start_run(self, workflow_name: str, input: Any = None) -> dict:
        return self._request("POST", f"/v1/workflows/{workflow_name}/runs", {"input": input or {}})

    def get_run(self, run_id: str) -> dict:
        return self._request("GET", f"/v1/runs/{run_id}")

    def list_runs(self, workflow_name: Optional[str] = None, limit: int = 50) -> list:
        query = f"?limit={limit}"
        if workflow_name:
            query += f"&workflow={workflow_name}"
        return self._request("GET", f"/v1/runs{query}")

    def get_history(self, run_id: str) -> list:
        return self._request("GET", f"/v1/runs/{run_id}/history")

    def cancel_run(self, run_id: str) -> dict:
        return self._request("POST", f"/v1/runs/{run_id}/cancel")

    def poll_task(self, workflow_name: str, worker_id: str) -> Optional[Task]:
        body = self._request("POST", "/v1/tasks/poll", {"workflow": workflow_name, "worker_id": worker_id})
        return _task_from_json(body) if body else None

    def heartbeat(self, step_id: str, lease_token: str) -> None:
        self._request("POST", f"/v1/tasks/{step_id}/heartbeat", {"lease_token": lease_token})

    def complete_task(self, step_id: str, lease_token: str, result: Any) -> None:
        self._request("POST", f"/v1/tasks/{step_id}/complete", {"lease_token": lease_token, "result": result})

    def fail_task(self, step_id: str, lease_token: str, reason: str) -> None:
        self._request("POST", f"/v1/tasks/{step_id}/fail", {"lease_token": lease_token, "reason": reason})
