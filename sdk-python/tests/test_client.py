import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from flowforge import Client, LeaseLost, RunNotFound


class FakeEngineHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        return json.loads(raw) if raw else None

    def _respond(self, status, body=None):
        payload = json.dumps(body).encode("utf-8") if body is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_POST(self):
        body = self._read_json()

        if self.path == "/v1/workflows":
            self._respond(201, body)
            return

        if self.path == "/v1/workflows/order_pipeline/runs":
            self._respond(201, {"id": "run-1", "workflow_name": "order_pipeline", "status": "RUNNING", "input": body.get("input")})
            return

        if self.path == "/v1/tasks/poll":
            if body.get("workflow") == "demo":
                self._respond(200, {
                    "step_id": "step-1", "run_id": "run-1", "step_name": "charge",
                    "is_compensation": False, "compensation_of": "", "attempt": 1, "max_attempts": 3,
                    "timeout_seconds": 30, "idempotency_key": "run-1:charge", "lease_token": "good",
                    "input": {"amount": 10}, "context": {},
                })
            else:
                self._respond(204)
            return

        if self.path == "/v1/tasks/step-1/complete":
            if body.get("lease_token") == "good":
                self._respond(200, {"status": "completed"})
            else:
                self._respond(409, {"error": "lease token does not match or step is not leased"})
            return

        if self.path == "/v1/runs/run-1/cancel":
            self._respond(200, {"status": "cancelling"})
            return

        self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/v1/runs/run-1":
            self._respond(200, {"id": "run-1", "status": "RUNNING", "steps": []})
            return
        if self.path == "/v1/runs/missing":
            self._respond(404, {"error": "run not found"})
            return
        self._respond(404, {"error": "not found"})


@pytest.fixture()
def server():
    httpd = HTTPServer(("127.0.0.1", 0), FakeEngineHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    thread.join()


@pytest.fixture()
def client(server):
    port = server.server_address[1]
    return Client(f"http://127.0.0.1:{port}")


def test_start_run_round_trips_input(client):
    run = client.start_run("order_pipeline", {"order_id": 7})
    assert run["id"] == "run-1"
    assert run["input"] == {"order_id": 7}


def test_get_run_not_found_raises_typed_error(client):
    with pytest.raises(RunNotFound):
        client.get_run("missing")


def test_poll_task_returns_none_on_no_content(client):
    assert client.poll_task("unregistered_workflow", "worker-1") is None


def test_poll_task_parses_task_fields(client):
    task = client.poll_task("demo", "worker-1")
    assert task.step_id == "step-1"
    assert task.step_name == "charge"
    assert task.attempt == 1
    assert task.lease_token == "good"
    assert task.context == {}


def test_complete_task_with_stale_lease_raises_lease_lost(client):
    with pytest.raises(LeaseLost):
        client.complete_task("step-1", "stale", {"ok": True})


def test_complete_task_with_valid_lease_succeeds(client):
    client.complete_task("step-1", "good", {"ok": True})  # no exception


def test_cancel_run(client):
    result = client.cancel_run("run-1")
    assert result["status"] == "cancelling"
