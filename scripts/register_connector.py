"""Register (or update) the Debezium Postgres connector and wait until it runs.

Uses PUT /connectors/<name>/config, which is an upsert, so running this
script twice is safe.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
NAME = "shop-postgres-cdc"
CONFIG_FILE = Path(__file__).resolve().parent.parent / "debezium" / "shop-postgres-connector.json"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict | list | None]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def wait_for_connect(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status, _ = call("GET", "/connectors")
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        print("waiting for Kafka Connect...")
        time.sleep(3)
    sys.exit("Kafka Connect did not become ready in time")


def main() -> None:
    wait_for_connect()
    config = json.loads(CONFIG_FILE.read_text())
    status, body = call("PUT", f"/connectors/{NAME}/config", config)
    if status not in (200, 201):
        sys.exit(f"failed to register connector ({status}): {body}")
    print(f"connector {NAME} registered")

    for _ in range(60):
        status, body = call("GET", f"/connectors/{NAME}/status")
        if status == 200 and body:
            states = [body["connector"]["state"]] + [t["state"] for t in body.get("tasks", [])]
            print("connector state:", states)
            if "FAILED" in states:
                sys.exit(f"connector failed: {json.dumps(body, indent=2)}")
            if len(states) > 1 and all(s == "RUNNING" for s in states):
                return
        time.sleep(2)
    sys.exit("connector did not reach RUNNING")


if __name__ == "__main__":
    main()
