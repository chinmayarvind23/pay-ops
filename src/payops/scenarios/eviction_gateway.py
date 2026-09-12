"""Fixed isolated-cluster reads and journaled kubelet configuration transitions."""

import json
import socket
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_gateway import ConcurrencyGateway
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.eviction_contract import NAMESPACE, NODE, victim_pod
from payops.tools.traces import bounded_read

CONFIG_PATH = "/var/lib/kubelet/config.yaml"


class EvictionGateway(ConcurrencyGateway):
    """The isolated context cannot reach the normal sandbox or its data namespace."""

    def __init__(self, kubeconfig: Path) -> None:
        """Pin the disposable cluster explicitly without modifying the user's global context."""
        super().__init__(kubeconfig)
        self._prefix = (
            *self._prefix[:4],
            "kind-payops-eviction",
            "--namespace",
            NAMESPACE,
            "--request-timeout=10s",
        )

    def verify_scope(self) -> JsonObject:
        """Only one specifically named kind container and node can be configured."""
        nodes = self._json(("get", "nodes"))
        if [object_value(p["metadata"])["name"] for p in object_items(nodes["items"])] != [NODE]:
            raise ValueError("eviction node scope mismatch")
        container = json.loads(bounded_read(("docker", "inspect", NODE), 65536, 10))[0]
        if container["Config"]["Labels"].get("io.x-k8s.kind.cluster") != "payops-eviction":
            raise ValueError("eviction Docker owner mismatch")
        return {"node": NODE, "container_id": container["Id"], "nodes": nodes}

    def config_text(self) -> str:
        """Read only the kubelet configuration, never certificates or credential contents."""
        return bounded_read(("docker", "exec", NODE, "cat", CONFIG_PATH), 32768, 10).decode()

    def replace_config(self, expected: str, new: str, container_id: str) -> None:
        """The canonical latch and exact original bytes guard a fixed node-local file transition."""
        if self.verify_scope()["container_id"] != container_id or self.config_text() != expected:
            raise ValueError("isolated kubelet changed outside journal")
        if len(new.encode()) > 32768:
            raise ValueError("kubelet configuration exceeds bound")
        subprocess.run(
            ("docker", "exec", "-i", NODE, "tee", CONFIG_PATH),
            input=new.encode("utf-8"),
            capture_output=True,
            check=True,
            timeout=10,
        )
        subprocess.run(
            ("docker", "exec", NODE, "systemctl", "restart", "kubelet"),
            capture_output=True,
            check=True,
            timeout=30,
        )

    def write(self, args: tuple[str, ...], body: JsonObject) -> JsonObject:
        """Internal fixed callers send small structured API objects on stdin."""
        self.verify_scope()
        result = subprocess.run(
            (*self._prefix, *args),
            input=json.dumps(body),
            text=True,
            capture_output=True,
            check=True,
            timeout=15,
        )
        if len(result.stdout.encode()) > 262144:
            raise ValueError("eviction API response capped")
        return JSON_OBJECT.validate_json(result.stdout)

    def namespace(self, run_id: str) -> JsonObject:
        """Create-only refuses any pre-existing namespace instead of adopting its contents."""
        victim_pod(run_id)
        return self.write(
            ("create", "-f", "-", "-o", "json"),
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": NAMESPACE,
                    "labels": {
                        "app.kubernetes.io/part-of": "payops",
                        "payops.dev/eviction-run": run_id,
                    },
                },
            },
        )

    def create_victim(self, run_id: str, recovered: bool = False) -> JsonObject:
        """Create one fixed synthetic victim with no token, real payment or ledger access."""
        return self.write(("create", "-f", "-", "-o", "json"), victim_pod(run_id, recovered))

    def pod(self, recovered: bool = False) -> JsonObject:
        """Only the two fixed experiment Pod names can be read here."""
        return self._json(("get", "pod", "victim-recovered" if recovered else "victim-control"))

    def observation(self) -> JsonObject:
        """Keep node conditions, effective config, memory signals and pod events together."""
        try:
            return self._observation()
        except subprocess.SubprocessError as error:
            return {"acquisition_error": type(error).__name__}

    def _observation(self) -> JsonObject:
        """Kubelet startup read failures remain explicit observations for bounded retry."""
        prefix = "/api/v1/nodes/" + NODE + "/proxy/"
        return {
            "nodes": self._json(("get", "nodes")),
            "pods": self._json(("get", "pods")),
            "events": self._json(("get", "events")),
            "stats": self._raw_json(prefix + "stats/summary"),
            "configz": self._raw_json(prefix + "configz"),
        }

    def _raw_json(self, path: str) -> JsonObject:
        """Internal fixed proxy paths use the same byte and deadline bounds as other reads."""
        raw = bounded_read((*self._prefix, "get", "--raw", path), 262144, 12)
        if len(raw) >= 262144:
            raise ValueError("eviction observation capped")
        return JSON_OBJECT.validate_json(raw)

    def healthy_victim(self, recovered: bool = False) -> JsonObject:
        """A real webhook request verifies service health before and after eviction."""
        name = "victim-recovered" if recovered else "victim-control"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 18084))
        process = subprocess.Popen(
            (*self._prefix, "port-forward", "pod/" + name, "18084:8080", "--address", "127.0.0.1"),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            with httpx.Client(timeout=3, trust_env=False) as client:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        response = client.post(
                            "http://127.0.0.1:18084/simulate",
                            json={"sample_id": "synthetic-" + uuid4().hex},
                        )
                        return {"status": response.status_code, "body": response.json()}
                    except httpx.RequestError:
                        time.sleep(0.1)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        diagnostic = process.stderr.read(8192).decode(errors="replace") if process.stderr else ""
        return {
            "acquisition_error": "victim HTTP forward unavailable",
            "forward_error": diagnostic,
            "pod": self.pod(recovered),
            "logs": bounded_read(
                (
                    *self._prefix,
                    "logs",
                    name,
                    "--container=sandbox",
                    "--tail=40",
                    "--limit-bytes=16384",
                ),
                16384,
                10,
            ).decode(errors="replace"),
        }

    def remove_namespace(self, expected: JsonObject, run_id: str) -> None:
        """Delete the created namespace under UID and version preconditions."""
        current = self._json(("get", "namespace", NAMESPACE))
        metadata = object_value(current["metadata"])
        if (
            metadata["uid"] != object_value(expected["metadata"])["uid"]
            or object_value(metadata["labels"]).get("payops.dev/eviction-run") != run_id
        ):
            raise ValueError("eviction namespace ownership changed")
        self.write(
            ("delete", "--raw", "/api/v1/namespaces/" + NAMESPACE, "-f", "-"),
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "propagationPolicy": "Foreground",
                "preconditions": {
                    "uid": metadata["uid"],
                    "resourceVersion": metadata["resourceVersion"],
                },
            },
        )

    def namespace_absence(self) -> JsonObject:
        """Keep the final namespace inventory as cleanup evidence."""
        return self._json(("get", "namespaces"))
