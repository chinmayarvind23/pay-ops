"""Reject manifest regressions that could expose or overprivilege the synthetic sandbox."""

import json
from pathlib import Path
from typing import Any

import yaml

MANIFESTS = Path(__file__).parents[2] / "infra" / "kubernetes" / "local"


def resources() -> list[dict[str, Any]]:
    """Read deployable resources while excluding kind's cluster-provider configuration."""
    return [
        item
        for name in ("namespace.yaml", "services.yaml")
        for item in yaml.safe_load_all((MANIFESTS / name).read_text(encoding="utf-8"))
    ]


def test_runtime_cannot_acquire_cluster_or_host_privileges() -> None:
    """Every workload must remain non-root, tokenless and unable to escalate privileges."""
    deployments = [item for item in resources() if item["kind"] == "Deployment"]
    assert len(deployments) == 5
    for deployment in deployments:
        pod = deployment["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False
        assert not any(pod.get(key, False) for key in ("hostNetwork", "hostPID", "hostIPC"))
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert pod["securityContext"]["runAsUser"] > 0
        assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
        assert all("hostPath" not in volume for volume in pod["volumes"])
        for container in pod["containers"]:
            security = container["securityContext"]
            assert security["allowPrivilegeEscalation"] is False
            assert security["readOnlyRootFilesystem"] is True
            assert security["capabilities"]["drop"] == ["ALL"]
            assert not security.get("privileged", False)


def test_ingress_is_internal_and_namespace_rejects_privileged_workloads() -> None:
    """ClusterIP and restricted admission prevent accidental public or privileged exposure."""
    documents = resources()
    namespace = next(item for item in documents if item["kind"] == "Namespace")
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    for item in documents:
        if item["kind"] != "Namespace":
            assert item["metadata"]["namespace"] == "payops-sandbox"
        if item["kind"] == "Service":
            assert item["spec"]["type"] == "ClusterIP"
            assert "externalIPs" not in item["spec"]
        if item["kind"] == "ServiceAccount":
            assert item["automountServiceAccountToken"] is False
    cluster = yaml.safe_load((MANIFESTS / "kind.yaml").read_text(encoding="utf-8"))
    assert cluster["networking"]["apiServerAddress"] == "127.0.0.1"
    assert all("extraPortMappings" not in node for node in cluster["nodes"])


def test_every_runtime_has_resource_and_probe_bounds() -> None:
    """Fault injection must remain resource bounded and health checks must terminate."""
    for item in resources():
        if item["kind"] != "Deployment":
            continue
        container = item["spec"]["template"]["spec"]["containers"][0]
        for boundary in ("requests", "limits"):
            assert {"cpu", "memory", "ephemeral-storage"} <= container["resources"][boundary].keys()
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            assert 0 < container[probe]["timeoutSeconds"] <= 2
            assert 0 < container[probe]["failureThreshold"] <= 30
            assert container[probe]["httpGet"]["path"] == "/health"


def test_synthetic_dependencies_resolve_to_declared_internal_services() -> None:
    """A configuration typo must not send synthetic traffic to an external endpoint."""
    documents = resources()
    services = {item["metadata"]["name"] for item in documents if item["kind"] == "Service"}
    allowed = {f"http://{name}.payops-sandbox.svc.cluster.local:8080" for name in services}
    for item in documents:
        if item["kind"] != "Deployment":
            continue
        container = item["spec"]["template"]["spec"]["containers"][0]
        environment = {entry["name"]: entry["value"] for entry in container["env"]}
        configuration = json.loads(environment["PAYOPS_SANDBOX_CONFIG"])
        assert set(configuration.values()) <= allowed
        assert container["imagePullPolicy"] == "Never"
