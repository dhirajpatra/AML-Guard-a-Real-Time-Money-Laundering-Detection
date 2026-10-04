"""Send one test trace to Langfuse (stdlib only) to prove the server + keys work.

    python scripts/langfuse_smoke.py        # host: http://localhost:3001
"""
from __future__ import annotations

import base64
import json
import os
import sys
import urllib.request
import uuid
from datetime import datetime, timezone

HOST = os.getenv("LANGFUSE_HOST_URL", "http://localhost:3001")
PK = os.getenv("LANGFUSE_PUBLIC_KEY", "pk-lf-aml-guard")
SK = os.getenv("LANGFUSE_SECRET_KEY", "sk-lf-aml-guard")


def build_batch(name: str = "aml-guard smoke test") -> dict:
    now = datetime.now(timezone.utc).isoformat()
    trace_id = str(uuid.uuid4())
    return {"batch": [
        {"id": str(uuid.uuid4()), "type": "trace-create", "timestamp": now,
         "body": {"id": trace_id, "name": name, "input": "ping", "output": "pong",
                  "tags": ["smoke"], "timestamp": now}},
        {"id": str(uuid.uuid4()), "type": "generation-create", "timestamp": now,
         "body": {"id": str(uuid.uuid4()), "traceId": trace_id, "name": "fake-llm-call", "model": "none",
                  "input": "ping", "output": "pong", "startTime": now, "endTime": now}},
    ]}


def main() -> int:
    auth = base64.b64encode(f"{PK}:{SK}".encode()).decode()
    with urllib.request.urlopen(f"{HOST}/api/public/health", timeout=10) as r:
        print("health:", r.status, r.read().decode())
    req = urllib.request.Request(f"{HOST}/api/public/ingestion", data=json.dumps(build_batch()).encode(),
                                 headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        print("ingestion:", r.status, r.read().decode()[:300])
    print(f"Open {HOST} -> project 'AML-Guard' -> Traces")
    return 0


if __name__ == "__main__":
    sys.exit(main())
