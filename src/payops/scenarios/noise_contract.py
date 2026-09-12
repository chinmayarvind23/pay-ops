"""A fixed independent CPU worker provides a measured distractor during processor outage."""

import json
from datetime import datetime

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_job import load_job

# This operator-owned script has no network, filesystem writes, or request-controlled code.
NOISE_SCRIPT = """import hashlib,json,time
from datetime import UTC,datetime
start=time.monotonic()
previous=start
while time.monotonic()-start < 120:
    hashlib.sha256(b"payops-unrelated-cpu"*4096).digest()
    now=time.monotonic()
    if now-previous >= 1:
        print(json.dumps({"event":"synthetic.cpu_noise","at":datetime.now(UTC).isoformat(),"elapsed":now-start,"cpu_seconds":time.process_time()}),flush=True)
        previous=now
"""


def noise_job(run_id: str) -> JsonObject:
    """Reuse the hardened no-retry envelope with fixed local CPU work."""
    job = load_job(run_id)
    object_value(job["metadata"])["name"] = "cpu-noise-" + run_id
    labels: JsonObject = {"app.kubernetes.io/part-of": "payops", "payops.dev/noise-run": run_id}
    object_value(job["metadata"])["labels"] = labels
    spec = object_value(job["spec"])
    spec["activeDeadlineSeconds"] = 150
    object_value(object_value(spec["template"])["metadata"])["labels"] = dict(labels)
    pod = object_value(object_value(spec["template"])["spec"])
    item = object_items(pod["containers"])[0]
    item["image"] = "payops-sandbox:dependencies"
    item["command"] = ["python", "-c", NOISE_SCRIPT]
    item.pop("env")
    object_value(object_value(item["resources"])["requests"])["cpu"] = "50m"
    return job


def noise_window(raw: str, started: datetime, completed: datetime) -> JsonObject:
    """Require measured CPU consumption spanning the full HTTP request window."""
    rows = [object_value(json.loads(line)) for line in raw.splitlines() if line.strip()]
    if not 2 <= len(rows) <= 121:
        raise ValueError("CPU noise has no bounded measurement window")
    for a, b in zip(rows, rows[1:], strict=False):
        if (
            a.get("event") != "synthetic.cpu_noise"
            or b.get("event") != "synthetic.cpu_noise"
            or datetime.fromisoformat(str(a["at"])) >= datetime.fromisoformat(str(b["at"]))
            or float(str(a["elapsed"])) >= float(str(b["elapsed"]))
            or float(str(a["cpu_seconds"])) >= float(str(b["cpu_seconds"]))
        ):
            raise ValueError("CPU noise counters or clocks do not advance")
    before = [row for row in rows if datetime.fromisoformat(str(row["at"])) <= started]
    after = [row for row in rows if datetime.fromisoformat(str(row["at"])) >= completed]
    if not before or not after:
        raise ValueError("CPU noise does not cover request window")
    a, b = before[-1], after[0]
    wall = float(str(b["elapsed"])) - float(str(a["elapsed"]))
    cpu = float(str(b["cpu_seconds"])) - float(str(a["cpu_seconds"]))
    if not 0 < wall <= 45 or not 0.2 <= cpu / wall <= 1:
        raise ValueError("CPU noise did not sustain measured demand")
    return {"start": a, "end": b, "cpu_cores": cpu / wall, "request_cpu_cores": 0.05}
