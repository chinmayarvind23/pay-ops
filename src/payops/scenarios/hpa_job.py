"""Fixed operator-created Job envelope for Service-routed HPA load."""

import re

from payops.scenarios.contracts import JsonObject

IMAGE = "payops-sandbox:hpa-paced"
RUNTIME_IMAGE_DIGEST = "sha256:4b793c101fd8127ed7d99d5315252539514607e4513877acd8b81867027bb5e6"


def load_job(run_id: str) -> JsonObject:
    """A run label binds acquisition and cleanup; code and destinations stay fixed."""
    if re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
        raise ValueError("HPA load requires a 32-character run identity")
    labels: JsonObject = {"app.kubernetes.io/part-of": "payops", "payops.dev/hpa-run": run_id}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "hpa-load-" + run_id, "namespace": "payops-sandbox", "labels": labels},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 210,
            "parallelism": 1,
            "completions": 1,
            "template": {
                "metadata": {"labels": dict(labels)},
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 5,
                    "serviceAccountName": "payments-api",
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "load",
                            "image": IMAGE,
                            "imagePullPolicy": "Never",
                            "command": ["python", "-m", "payops.scenarios.hpa_load"],
                            "env": [{"name": "PAYOPS_HPA_LOAD", "value": "kind-v1"}],
                            "resources": {
                                "requests": {
                                    "cpu": "100m",
                                    "memory": "96Mi",
                                    "ephemeral-storage": "32Mi",
                                },
                                "limits": {
                                    "cpu": "500m",
                                    "memory": "256Mi",
                                    "ephemeral-storage": "64Mi",
                                },
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [{"name": "work", "mountPath": "/tmp"}],
                        }
                    ],
                    "volumes": [{"name": "work", "emptyDir": {"sizeLimit": "16Mi"}}],
                },
            },
        },
    }
