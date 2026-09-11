"""Fixed local kubectl operations belong to the trusted operator harness only."""

import json
import re
import shutil
import socket
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx
from pydantic import JsonValue, TypeAdapter

from payops.scenarios.contracts import DeploymentName, JsonObject, object_items, object_value


class KubectlGateway:
    """Every invocation pins context, namespace and a short API request deadline."""

    mode: Literal["local_kind", "fixture_replay"] = "local_kind"

    def __init__(self, kubeconfig: Path) -> None:
        """Resolve the explicit configuration once; never select a global context."""
        executable = shutil.which("kubectl")
        if executable is None or not kubeconfig.is_file():
            raise ValueError("kubectl and explicit local kubeconfig are required")
        self._prefix = (
            executable,
            "--kubeconfig",
            str(kubeconfig.resolve()),
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=10s",
        )

    def _invoke(self, args: tuple[str, ...]) -> str:
        """Structured argv cannot be interpreted as shell commands or substitutions."""
        result = subprocess.run(
            (*self._prefix, *args),
            shell=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=True,
        )
        return result.stdout

    def _json(self, args: tuple[str, ...]) -> JsonObject:
        """Validate API JSON before any nested resource assumptions are used."""
        return TypeAdapter[JsonObject](JsonObject).validate_json(
            self._invoke((*args, "-o", "json"))
        )

    def verify_scope(self) -> JsonObject:
        """Loopback API, dedicated node names and namespace ownership prevent scope drift."""
        server = self._invoke(
            (
                "config",
                "view",
                "--minify",
                "-o",
                "jsonpath={.clusters[0].cluster.server}",
            )
        )
        if re.fullmatch(r"https://127\.0\.0\.1:[0-9]+", server) is None:
            raise ValueError("scenario API must be loopback")
        nodes = self._json(("get", "nodes"))
        names = {
            str(object_value(item["metadata"])["name"]) for item in object_items(nodes["items"])
        }
        if names != {"payops-dev-control-plane", "payops-dev-worker"}:
            raise ValueError("unexpected kind node identity")
        namespace = self._json(("get", "namespace", "payops-sandbox"))
        labels = object_value(object_value(namespace["metadata"]).get("labels", {}))
        if labels.get("app.kubernetes.io/part-of") != "payops":
            raise ValueError("namespace is not owned by PayOps")
        return {
            "server": server,
            "nodes": list[JsonValue](sorted(names)),
            "namespace": "payops-sandbox",
        }

    def deployment(self, name: DeploymentName) -> JsonObject:
        """Only reviewed scenario targets can be read through this adapter."""
        self.validate_target(name)
        return self._json(("get", "deployment", name))

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """JSON Patch tests prevent replacing another object or a concurrently changed spec."""
        self.validate_target(name)
        self.verify_scope()
        current = self.deployment(name)
        metadata = object_value(current["metadata"])
        if (
            current["spec"] != expected["spec"]
            or metadata["uid"] != object_value(expected["metadata"])["uid"]
        ):
            raise ValueError("Deployment changed before patch")
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": metadata["resourceVersion"],
            },
            {"op": "test", "path": "/spec", "value": expected["spec"]},
            {"op": "replace", "path": "/spec", "value": spec},
        ]
        self._invoke(("patch", "deployment", name, "--type=json", "--patch", json.dumps(patch)))

    def observe(self, name: DeploymentName) -> JsonObject:
        """Events are filtered to current target pod UIDs, excluding stale distractions."""
        self.validate_target(name)
        pods = object_items(
            self._json(
                (
                    "get",
                    "pods",
                    "-l",
                    f"app.kubernetes.io/name={name}",
                )
            )["items"]
        )
        uids = {str(object_value(pod["metadata"]).get("uid")) for pod in pods}
        events = object_items(
            self._json(
                (
                    "get",
                    "events",
                    "--field-selector",
                    "involvedObject.kind=Pod",
                )
            )["items"]
        )
        relevant = [
            event
            for event in events
            if str(object_value(event["involvedObject"]).get("uid")) in uids
        ]
        return {
            "deployment": self.deployment(name),
            "pods": list[JsonValue](pods),
            "events": list[JsonValue](relevant),
        }

    @staticmethod
    def validate_target(name: str) -> None:
        """Runtime callers cannot bypass the closed type alias with arbitrary resources."""
        if name not in {"payments-api", "processor-adapter", "webhook-sim"}:
            raise ValueError("Deployment outside scenario allowlist")

    @contextmanager
    def _forward(self) -> Generator[str]:
        """Own a fresh loopback forward because a rollout invalidates the old selected pod."""
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 18081))
        process = subprocess.Popen(
            (
                *self._prefix,
                "port-forward",
                "service/payments-api",
                "18081:8080",
                "--address",
                "127.0.0.1",
            ),
            shell=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            yield "http://127.0.0.1:18081"
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def healthy(self) -> JsonObject:
        """A fresh sample proves current peer connectivity, not cached prior success."""
        with self._forward() as origin:
            deadline = time.monotonic() + 12
            with httpx.Client(timeout=3, trust_env=False, follow_redirects=False) as client:
                while time.monotonic() < deadline:
                    try:
                        response = client.post(
                            origin + "/simulate",
                            json={"sample_id": f"synthetic-{uuid4().hex}"},
                        )
                        return {"sample_status": response.status_code, "sample_body": response.text}
                    except httpx.RequestError:
                        time.sleep(0.25)
        raise TimeoutError("local synthetic HTTP probe unavailable")
