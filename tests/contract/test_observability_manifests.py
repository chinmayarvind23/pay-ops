"""Keep local metric collection bounded without granting infrastructure authority."""

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).parents[2] / "infra" / "kubernetes" / "observability"


def documents() -> list[dict[str, Any]]:
    """Inspect only applied Kubernetes objects, leaving generated config separately validated."""
    return [
        item
        for name in ("namespace.yaml", "prometheus.yaml")
        for item in yaml.safe_load_all((ROOT / name).read_text(encoding="utf-8"))
    ]


def test_prometheus_has_no_cluster_credentials_or_public_listener() -> None:
    """Static scraping must not accidentally acquire discovery or mutation authority."""
    for item in documents():
        assert item["kind"] not in {"ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding"}
        if item["kind"] == "Service":
            assert item["spec"]["type"] == "ClusterIP"
            assert "externalIPs" not in item["spec"]
        if item["kind"] == "ServiceAccount":
            assert item["automountServiceAccountToken"] is False
        if item["kind"] != "Namespace":
            assert item["metadata"]["namespace"] == "payops-observability"
    deployment = next(item for item in documents() if item["kind"] == "Deployment")
    pod = deployment["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    assert not pod.get("hostNetwork", False)
    assert all("hostPath" not in volume for volume in pod["volumes"])
    container = pod["containers"][0]
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert not any("--web.enable-" in argument for argument in container["args"])


def test_scrape_targets_are_only_five_internal_payment_services() -> None:
    """Fixed destinations and scrape budgets prevent arbitrary retrieval and cardinality growth."""
    config = yaml.safe_load((ROOT / "prometheus.yml").read_text(encoding="utf-8"))
    assert config["global"]["scrape_timeout"] == "2s"
    assert len(config["scrape_configs"]) == 1
    job = config["scrape_configs"][0]
    assert "kubernetes_sd_configs" not in job
    assert "authorization" not in job
    assert job["sample_limit"] <= 2000
    assert job["label_limit"] <= 12
    services = {"payments-api", "risk-sim", "ledger-sim", "processor-adapter", "webhook-sim"}
    assert {target["labels"]["service"] for target in job["static_configs"]} == services
    for target in job["static_configs"]:
        service = target["labels"]["service"]
        assert target["targets"] == [f"{service}.payops-sandbox.svc.cluster.local:8080"]


def test_storage_runtime_and_query_costs_are_bounded() -> None:
    """Collection must survive a pod restart without exhausting the local experiment host."""
    deployment = next(item for item in documents() if item["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert "@sha256:" in container["image"]
    assert "--storage.tsdb.retention.size=256MB" in container["args"]
    assert "--query.timeout=10s" in container["args"]
    assert "--query.max-concurrency=4" in container["args"]
    for boundary in ("requests", "limits"):
        assert {"cpu", "memory", "ephemeral-storage"} <= container["resources"][boundary].keys()
    claim = next(item for item in documents() if item["kind"] == "PersistentVolumeClaim")
    assert claim["spec"]["resources"]["requests"]["storage"] == "1Gi"
