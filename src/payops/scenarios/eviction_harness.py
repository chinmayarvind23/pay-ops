"""Journal a real kubelet memory-pressure eviction on an isolated disposable kind node."""

from collections.abc import Callable
from pathlib import Path

import yaml

from payops.scenarios.concurrency_harness import ConcurrencyHarness, ConcurrencyRun
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.dependency_specs import RUNTIME_IMAGE_DIGEST
from payops.scenarios.eviction_contract import NAMESPACE, evicted, pressure_config
from payops.scenarios.eviction_gateway import EvictionGateway
from payops.scenarios.runner import CleanupUnverified


class EvictionHarness(ConcurrencyHarness):
    """The normal sandbox and its data services are outside this gateway's fixed context."""

    access: EvictionGateway

    def __init__(self, kubeconfig: Path, evidence_root: Path) -> None:
        """Reuse the canonical cross-experiment latch and immutable receipt recovery machinery."""
        super().__init__(kubeconfig, evidence_root, EvictionGateway(kubeconfig))
        self.namespace_document: JsonObject | None = None

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> ConcurrencyRun:
        """Capture the exact node container and configuration before creating any victim."""
        scope = self.access.verify_scope()
        self._save(directory, receipt, "scope", scope)
        namespaces = self.access.namespace_absence()
        self._save(directory, receipt, "namespace-before", namespaces)
        if not namespace_absent(namespaces):
            raise ValueError("eviction namespace already exists")
        original: JsonObject = {
            "config": self.access.config_text(),
            "container_id": scope["container_id"],
        }
        return ConcurrencyRun(original, {}, {})

    def _restore_config(self, context: ConcurrencyRun) -> None:
        """Restore bytes only from the known injected variant; never overwrite a foreign edit."""
        current = self.access.config_text()
        original = str(context.original["config"])
        if current != original:
            if current != context.enabled.get("config"):
                raise CleanupUnverified("isolated kubelet configuration changed outside journal")
            self.access.replace_config(current, original, str(context.original["container_id"]))

    def _restore_original(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Attempt config and namespace cleanup; retain the latch on either failure."""
        errors: list[str] = []
        try:
            self._restore_config(context)
            if self.access.config_text() != context.original["config"]:
                raise CleanupUnverified("original kubelet bytes were not restored")
            self._save(directory, receipt, "config-restored", {"config": self.access.config_text()})
        except Exception as error:
            errors.append(str(error))
        try:
            if self.namespace_document is not None:
                self.access.remove_namespace(self.namespace_document, receipt.run_id)
                self._wait(
                    directory,
                    receipt,
                    "namespace-removed",
                    self.access.namespace_absence,
                    namespace_absent,
                )
        except Exception as error:
            errors.append(str(error))
        if errors:
            raise CleanupUnverified("; ".join(errors))

    def _experiment(
        self,
        context: ConcurrencyRun,
        directory: Path,
        receipt: ScenarioReceipt,
        callback: Callable[[], None] | None,
    ) -> None:
        """Actual health, memory signals, kubelet eviction and restored health form the contrast."""
        self.namespace_document = self.access.namespace(receipt.run_id)
        self._save(directory, receipt, "namespace-created", self.namespace_document)
        victim = self.access.create_victim(receipt.run_id)
        self._save(directory, receipt, "victim-created", victim)
        self._wait(directory, receipt, "victim-ready", self.access.pod, victim_ready)
        self._wait(
            directory, receipt, "healthy-control", self.access.healthy_victim, webhook_healthy
        )
        receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        baseline = self.access.observation()
        self._save(directory, receipt, "baseline", baseline)
        if not node_recovered(baseline):
            raise ValueError("isolated node was not healthy before pressure")
        memory = object_value(object_value(object_value(baseline["stats"])["node"])["memory"])
        context.enabled["config"] = pressure_config(str(context.original["config"]), memory)
        self._save(
            directory,
            receipt,
            "config-transition",
            {"original": context.original, "enabled": context.enabled},
        )
        receipt.injection_requested_at = utc_timestamp()
        self.access.replace_config(
            str(context.original["config"]),
            str(context.enabled["config"]),
            str(context.original["container_id"]),
        )
        self._wait(
            directory,
            receipt,
            "eviction",
            self.access.observation,
            lambda observed: eviction_proof(observed, victim, str(context.enabled["config"])),
        )
        receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
        self._investigate(receipt, callback)
        self._restore_config(context)
        self._wait(directory, receipt, "node-recovered", self.access.observation, node_recovered)
        recovered = self.access.create_victim(receipt.run_id, True)
        self._save(directory, receipt, "recovered-created", recovered)
        self._wait(
            directory, receipt, "recovered-ready", lambda: self.access.pod(True), victim_ready
        )
        self._wait(
            directory,
            receipt,
            "healthy-recovered",
            lambda: self.access.healthy_victim(True),
            webhook_healthy,
        )

    def run(
        self, case_id: CaseId = "SCHED-04", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Fail closed on incomplete pressure, provenance, health or restoration evidence."""
        if case_id != "SCHED-04":
            raise ValueError("eviction harness accepts only SCHED-04")
        directory, receipt = self._start(case_id)
        context: ConcurrencyRun | None = None
        try:
            context = self._prepare(directory, receipt)
            self._save(directory, receipt, "journal", context.original)
            self._experiment(context, directory, receipt, after_activation)
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt)
        return receipt


def namespace_absent(document: JsonObject) -> bool:
    """Namespace disappearance proves both victim pods and their projections were removed."""
    return not any(
        object_value(item["metadata"])["name"] == NAMESPACE
        for item in object_items(document["items"])
    )


def victim_ready(pod: JsonObject) -> bool:
    """Only the actual pinned, unrestarted sandbox process can satisfy either health control."""
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    return (
        len(statuses) == 1
        and statuses[0].get("ready") is True
        and statuses[0].get("restartCount") == 0
        and str(statuses[0].get("imageID", "")).split("@")[-1] == RUNTIME_IMAGE_DIGEST
    )


def webhook_healthy(observed: JsonObject) -> bool:
    """A successful isolated webhook must return the expected explicitly synthetic result."""
    body = object_value(observed.get("body", {}))
    return (
        observed.get("status") == 200
        and body.get("role") == "webhook"
        and body.get("status") == "accepted"
        and body.get("synthetic") is True
    )


def node_recovered(observed: JsonObject) -> bool:
    """Require current Ready and no memory pressure, rather than just successful API access."""
    if "acquisition_error" in observed:
        return False
    nodes = object_items(object_value(observed["nodes"])["items"])
    if len(nodes) != 1:
        return False
    conditions = {
        str(item["type"]): item["status"]
        for item in object_items(object_value(nodes[0]["status"])["conditions"])
    }
    config = object_value(object_value(observed["configz"])["kubeletconfig"])
    return (
        conditions.get("Ready") == "True"
        and conditions.get("MemoryPressure") == "False"
        and "memory.available" not in object_value(config.get("evictionHard", {}))
    )


def eviction_proof(observed: JsonObject, original: JsonObject, enabled: str) -> bool:
    """Join the owned eviction to effective config, node pressure and memory signal."""
    if "acquisition_error" in observed:
        return False
    pods = object_items(object_value(observed["pods"])["items"])
    if len(pods) != 1 or not evicted(pods[0], original):
        return False
    expected = object_value(object_value(yaml.safe_load(enabled))["evictionHard"])[
        "memory.available"
    ]
    config = object_value(object_value(observed["configz"])["kubeletconfig"])
    memory = object_value(object_value(object_value(observed["stats"])["node"])["memory"])
    conditions = object_items(
        object_value(object_items(object_value(observed["nodes"])["items"])[0]["status"])[
            "conditions"
        ]
    )
    events = object_items(object_value(observed["events"])["items"])
    matching = [
        e
        for e in events
        if object_value(e.get("involvedObject", {})).get("uid")
        == object_value(original["metadata"])["uid"]
        and e.get("reason") == "Evicted"
    ]
    return (
        object_value(config.get("evictionHard", {})).get("memory.available") == expected
        and int(str(memory["availableBytes"])) < int(str(expected))
        and any(c.get("type") == "MemoryPressure" and c.get("status") == "True" for c in conditions)
        and bool(matching)
    )
