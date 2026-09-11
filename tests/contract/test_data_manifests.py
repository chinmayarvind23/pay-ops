"""Local data services must preserve namespace, credential, TLS and resource boundaries."""

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).parents[2] / "infra/kubernetes/data"


def objects() -> list[dict[str, Any]]:
    """Inspect the applied objects while keeping generated ConfigMaps separately readable."""
    return [
        item
        for name in ("namespace.yaml", "postgres.yaml", "redis.yaml", "elasticsearch.yaml")
        for item in yaml.safe_load_all((ROOT / name).read_text(encoding="utf-8"))
    ]


def containers() -> dict[str, dict[str, Any]]:
    """Pair each local service with its sole runtime container for boundary comparisons."""
    return {
        item["metadata"]["name"]: item["spec"]["template"]["spec"]["containers"][0]
        for item in objects()
        if item["kind"] == "Deployment"
    }


def test_namespace_credentials_and_public_access_are_closed() -> None:
    """Data services cannot gain Kubernetes authority or expose a public listener by default."""
    expected_ports = {5432, 6379, 9200}
    for item in objects():
        assert item["kind"] not in {
            "Secret",
            "Role",
            "RoleBinding",
            "ClusterRole",
            "ClusterRoleBinding",
        }
        if item["kind"] == "Namespace":
            assert item["metadata"]["name"] == "payops-data"
            assert item["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
        else:
            assert item["metadata"]["namespace"] == "payops-data"
        if item["kind"] == "ServiceAccount":
            assert item["automountServiceAccountToken"] is False
        if item["kind"] == "Service":
            assert item["spec"]["type"] == "ClusterIP"
            assert "externalIPs" not in item["spec"]
            assert item["spec"]["ports"][0]["port"] in expected_ports
            assert "nodePort" not in item["spec"]["ports"][0]


def test_runtime_and_storage_costs_are_bounded() -> None:
    """Single replicas have fixed resource ceilings, scratch caps and retained small claims."""
    claims = [item for item in objects() if item["kind"] == "PersistentVolumeClaim"]
    assert sorted(item["spec"]["resources"]["requests"]["storage"] for item in claims) == [
        "1Gi",
        "2Gi",
        "2Gi",
    ]
    for item in objects():
        if item["kind"] != "Deployment":
            continue
        assert item["spec"]["replicas"] == 1 and item["spec"]["strategy"]["type"] == "Recreate"
        pod = item["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
        assert not any(pod.get(key, False) for key in ("hostNetwork", "hostPID", "hostIPC"))
        for volume in pod["volumes"]:
            assert "hostPath" not in volume
            if "emptyDir" in volume:
                assert "sizeLimit" in volume["emptyDir"]
        for container in [*pod["containers"], *pod.get("initContainers", [])]:
            assert re.search(r"@sha256:[a-f0-9]{64}$", container["image"])
            assert container["securityContext"]["readOnlyRootFilesystem"] is True
            assert container["securityContext"]["allowPrivilegeEscalation"] is False
            assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
            for boundary in ("requests", "limits"):
                assert {"cpu", "memory", "ephemeral-storage"} <= container["resources"][
                    boundary
                ].keys()
    assert {name: c["resources"]["limits"]["memory"] for name, c in containers().items()} == {
        "postgres": "512Mi",
        "redis": "256Mi",
        "elasticsearch": "1536Mi",
    }


def test_each_data_protocol_requires_tls_and_authentication() -> None:
    """The non-enforcing kind CNI cannot substitute for authenticated encrypted service access."""
    postgres = (ROOT / "postgresql.conf").read_text()
    hba = (ROOT / "pg_hba.conf").read_text()
    redis = (ROOT / "redis.conf").read_text()
    elastic = yaml.safe_load((ROOT / "elasticsearch.yml").read_text())
    assert "ssl = on" in postgres and "password_encryption = 'scram-sha-256'" in postgres
    assert "hostssl all all 0.0.0.0/0 scram-sha-256" in hba
    assert "hostnossl all all 0.0.0.0/0 reject" in hba and "trust" not in hba
    assert "port 0\n" in redis and "tls-port 6379\n" in redis
    assert "aclfile /run/payops/users.acl" in redis
    assert elastic["xpack.security.enabled"] is True
    assert elastic["xpack.security.http.ssl.enabled"] is True
    assert elastic["transport.host"] == "127.0.0.1"
    for item in objects():
        if item["kind"] != "Deployment":
            continue
        pod = item["spec"]["template"]["spec"]
        secrets = [volume["secret"] for volume in pod["volumes"] if "secret" in volume]
        assert len(secrets) == 1 and secrets[0]["defaultMode"] == 0o440


def test_queries_probes_and_jvm_have_explicit_bounds() -> None:
    """Default SQL/search deadlines and cache size bounds keep local experiments finite."""
    postgres = (ROOT / "postgresql.conf").read_text()
    redis = (ROOT / "redis.conf").read_text()
    elastic = yaml.safe_load((ROOT / "elasticsearch.yml").read_text())
    assert "max_connections = 20" in postgres and "statement_timeout = 5s" in postgres
    assert "lock_timeout = 1s" in postgres and "temp_file_limit = 32MB" in postgres
    assert "maxmemory 128mb" in redis and "maxclients 64" in redis
    assert "proto-max-bulk-len 1mb" in redis and "maxmemory-policy allkeys-lru" in redis
    assert elastic["search.default_search_timeout"] == "5s"
    assert elastic["search.max_buckets"] == 1000 and elastic["http.max_content_length"] == "1mb"
    assert elastic["search.default_keep_alive"] == "30s"
    assert elastic["search.max_keep_alive"] == "1m"
    assert elastic["node.store.allow_mmap"] is False and elastic["xpack.ml.enabled"] is False
    for container in containers().values():
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            assert container[probe]["timeoutSeconds"] <= 3
            assert container[probe]["failureThreshold"] <= 90
    es = containers()["elasticsearch"]
    assert es["env"][0]["value"] == "-Xms768m -Xmx768m -XX:ActiveProcessorCount=2"
