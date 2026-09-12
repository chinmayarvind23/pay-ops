"""Run enumerated semantic mutants in isolated snapshots, never in the working source."""

import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Mutation:
    """Exact single replacements make each experiment independently reviewable."""

    name: str
    path: str
    before: str
    after: str
    test: str


CONTRACT = "src/payops/contracts/__init__.py"
ARTIFACT = "src/payops/evidence/artifacts.py"
NORMALIZE = "src/payops/evidence/normalize.py"
REDACT = "src/payops/evidence/redact.py"
METRIC = "src/payops/evaluation/metrics.py"
GRAPH = "src/payops/orchestrator/graph.py"
NODES = "src/payops/orchestrator/nodes.py"
POLICY = "src/payops/policy/engine.py"
POLICY_CONTRACT = "src/payops/policy/contracts.py"
RECORD = "src/payops/remediation/contracts.py"
STORE = "src/payops/remediation/store.py"
BROKER = "src/payops/remediation/broker.py"
BUDGET = "src/payops/orchestrator/budget.py"
REGISTRY = "src/payops/tools/registry.py"
VERIFY = "src/payops/evidence/verification.py"
SCHEMA_TESTS = "tests/unit/test_contracts.py tests/unit/test_lineage.py"
EVIDENCE_TESTS = "tests/unit/test_evidence.py"
METRIC_TESTS = "tests/unit/test_metrics.py"
GRAPH_TESTS = "tests/unit/test_graph.py"
POLICY_TESTS = "tests/unit/test_policy.py"
BROKER_TESTS = "tests/unit/test_remediation.py"
BUDGET_TESTS = "tests/unit/test_reasoning_budget.py"
REGISTRY_TESTS = "tests/unit/test_registry.py tests/unit/test_mutation_boundaries.py"
CORE_MUTATIONS = (
    Mutation("schema_extra_fields", CONTRACT, 'extra="forbid"', 'extra="allow"', SCHEMA_TESTS),
    Mutation("schema_frozen", CONTRACT, "frozen=True", "frozen=False", SCHEMA_TESTS),
    Mutation(
        "schema_aware_observation",
        CONTRACT,
        "observed_at: AwareDatetime",
        "observed_at: datetime",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_report_owner",
        CONTRACT,
        "self.report is not None and self.report.incident_id != self.incident_id",
        "False",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_duplicate_evidence",
        CONTRACT,
        "if len(ids) != len(self.evidence):",
        "if False:",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_unresolved_citation",
        CONTRACT,
        "if not (supports | refutes) <= ids:",
        "if False:",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_conflicting_citation", CONTRACT, "if supports & refutes:", "if False:", SCHEMA_TESTS
    ),
    Mutation(
        "schema_confidence_bound",
        CONTRACT,
        "ge=0, le=1, allow_inf_nan=False",
        "ge=0, le=2, allow_inf_nan=False",
        SCHEMA_TESTS,
    ),
    Mutation(
        "artifact_hash", ARTIFACT, "sha256(content).hexdigest() != digest", "False", EVIDENCE_TESTS
    ),
    Mutation(
        "artifact_metadata",
        ARTIFACT,
        'if payload.get("evidence") != expected:',
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "artifact_uri",
        ARTIFACT,
        'if evidence.artifact_uri != f"sha256://{digest}":',
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "artifact_no_overwrite",
        ARTIFACT,
        "os.link(temporary, path)",
        "os.replace(temporary, path)",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "context_verify", NORMALIZE, "        store.verify(item)", "        pass", EVIDENCE_TESTS
    ),
    Mutation(
        "context_incident",
        NORMALIZE,
        "if len({item.incident_id for item in evidence}) > 1:",
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "redaction_env",
        REDACT,
        'SENSITIVE_KEY.search(key) or (sensitive_name and key in {"value", "valueFrom"})',
        "SENSITIVE_KEY.search(key)",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "recall_denominator", METRIC, "total=len(gold)", "total=len(predictions)", METRIC_TESTS
    ),
    Mutation(
        "recall_equal_gate",
        METRIC,
        "self.fraction >= Fraction(required_hits, required_total)",
        "self.fraction > Fraction(required_hits, required_total)",
        METRIC_TESTS,
    ),
    Mutation(
        "recall_threshold_validation",
        METRIC,
        "required_total <= 0 or not 0 <= required_hits <= required_total",
        "required_total <= 0",
        METRIC_TESTS,
    ),
    Mutation(
        "recall_top_k",
        METRIC,
        "predictions.get(case, ())[:k]",
        "predictions.get(case, ())[:k + 1]",
        METRIC_TESTS,
    ),
    Mutation("recall_unknown_case", METRIC, "or set(predictions) - set(gold)", "", METRIC_TESTS),
    Mutation(
        "recall_duplicate_ranks",
        METRIC,
        "if any(len(ranking) != len(set(ranking)) for ranking in predictions.values()):",
        "if False:",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_deduplicate",
        METRIC,
        "unique = frozenset(predicted)",
        "unique = predicted",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_relation",
        METRIC,
        "int(link in gold)",
        "int(any(label.evidence_id == link.evidence_id for label in gold))",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_owner", METRIC, "or item.incident_id != link.incident_id", "", METRIC_TESTS
    ),
    Mutation(
        "attribution_alias", METRIC, "or item.evidence_id != link.evidence_id", "", METRIC_TESTS
    ),
    Mutation(
        "attribution_artifact",
        METRIC,
        "            verify_evidence(item, store)",
        "            pass",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_nested_lineage",
        METRIC,
        "            verify_evidence(item, store)",
        "            store.verify(item)",
        METRIC_TESTS,
    ),
    Mutation(
        "percentile_estimator",
        METRIC,
        "math.ceil(quantile * len(samples))",
        "math.floor(quantile * len(samples))",
        METRIC_TESTS,
    ),
    Mutation("percentile_negative", METRIC, "or sample < 0", "", METRIC_TESTS),
    Mutation("graph_attempt_limit", NODES, "used >= state.budget.max_steps", "False", GRAPH_TESTS),
    Mutation(
        "graph_attempt_durability",
        NODES,
        '    with (directory / f"attempt-{used + 1:02d}.json").open("xb") as stream:\n'
        "        stream.write(record.model_dump_json().encode())\n"
        "        stream.flush()\n"
        "        os.fsync(stream.fileno())",
        "    # Mutation: lose reservation before a possible operation crash.\n    del record",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_node_start_deadline",
        NODES,
        "or utc_now() >= state.budget.node_start_deadline",
        "",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_read_budget",
        NODES,
        "if reserved > state.budget.max_tool_calls:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_dispatch_exclusive",
        NODES,
        'with (output / "collection-dispatched").open("xb") as marker:',
        'with (output / "collection-dispatched").open("wb") as marker:',
        GRAPH_TESTS,
    ),
    Mutation("graph_retained_batch", NODES, "if result_path.exists():", "if False:", GRAPH_TESTS),
    Mutation(
        "graph_batch_owner",
        NODES,
        "if result.incident_id != state.incident.incident_id:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_integrity_outcome",
        NODES,
        '                    terminal="SECURITY_BLOCK",\n'
        '                    reasoning_stop_reason="SECURITY_BLOCK" '
        "if self.reasoner_factory else None,",
        '                    terminal="EVIDENCE_INSUFFICIENT",\n'
        '                    reasoning_stop_reason="SECURITY_BLOCK" '
        "if self.reasoner_factory else None,",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_namespace_scope",
        NODES,
        'request.namespace == "payops-sandbox" and request.service in SERVICES',
        "request.service in SERVICES",
        GRAPH_TESTS,
    ),
    Mutation("graph_report_mode", NODES, "mode=current.mode", 'mode="local_kind"', GRAPH_TESTS),
    Mutation(
        "graph_resume_mode", GRAPH, "if existing.mode != self.mode:", "if False:", GRAPH_TESTS
    ),
    Mutation(
        "graph_thread_identity",
        GRAPH,
        "if existing.incident != initial.incident:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_worker_lock",
        GRAPH,
        "FileLock(lock_path, timeout=0)",
        '__import__("contextlib").nullcontext()',
        GRAPH_TESTS,
    ),
)


POLICY_MUTATIONS = (
    Mutation("policy_disabled_identity", POLICY, "and principal.enabled", "and True", POLICY_TESTS),
    Mutation("policy_role", POLICY, "and role in principal.roles", "and True", POLICY_TESTS),
    Mutation(
        "policy_identity_scope",
        POLICY,
        "and namespace in principal.namespaces",
        "and True",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_identity_expiry", POLICY, "and principal.expires_at > now", "and True", POLICY_TESTS
    ),
    Mutation(
        "policy_identity_freshness",
        POLICY,
        "0 <= (now - principal.verified_at).total_seconds() <= 60",
        "True",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_service_scope", POLICY, "proposal.service not in SERVICES", "False", POLICY_TESTS
    ),
    Mutation(
        "policy_incident_identity",
        POLICY,
        "proposal.incident_id != incident.incident_id",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_resource_scope",
        POLICY,
        "(proposal.namespace, proposal.service) != (resource.namespace, resource.service)",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_proposal_mode", POLICY, "proposal.mode != resource.mode", "False", POLICY_TESTS
    ),
    Mutation("policy_synthetic", POLICY, "not resource.synthetic", "False", POLICY_TESTS),
    Mutation(
        "policy_resource_freshness",
        POLICY,
        "0 <= (now - resource.observed_at).total_seconds() <= 30",
        "True",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_preconditions",
        POLICY,
        "(proposal.resource_uid, proposal.expected_version) != (resource.uid, resource.version)",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_resource_kind", POLICY, "resource.kind != expected_kind", "False", POLICY_TESTS
    ),
    Mutation(
        "policy_revision_inventory",
        POLICY,
        "proposal.revision_sha256 not in resource.approved_revisions",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_evidence_mode",
        POLICY,
        "report.mode != context.resource.mode",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_observation_freshness",
        POLICY,
        "0 <= (now - item.observed_at).total_seconds() <= 300",
        "True",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_collection_freshness",
        POLICY,
        "0 <= (now - item.collected_at).total_seconds() <= 300",
        "True",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_artifact_integrity",
        POLICY,
        "        store.verify(item)",
        "        pass",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_retrieval_authority",
        POLICY,
        'item.source in {"RUNBOOK", "MEMORY"}',
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_payment_lineage",
        POLICY,
        "        window = verify_payment_window(item, store)",
        "        from payops.evidence.payment_window import PaymentWindow\n"
        '        window = PaymentWindow.model_validate(store.verify(item).get("payload"))',
        POLICY_TESTS,
    ),
    Mutation(
        "policy_payment_completeness",
        POLICY,
        'window.status != "complete"',
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_scale_ceiling",
        POLICY_CONTRACT,
        "Field(ge=1, le=3, strict=True)",
        "Field(ge=1, le=4, strict=True)",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_scale_strict",
        POLICY_CONTRACT,
        "Field(ge=1, le=3, strict=True)",
        "Field(ge=1, le=3, strict=False)",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_duplicate_citations",
        POLICY_CONTRACT,
        "len(self.evidence_ids) != len(set(self.evidence_ids))",
        "False",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_digest_parameters",
        POLICY,
        "sha256(payload.encode()).hexdigest()",
        "sha256(b'constant').hexdigest()",
        POLICY_TESTS,
    ),
    Mutation(
        "policy_positive_control",
        POLICY,
        'decision="APPROVAL_REQUIRED",',
        'decision="DENY",',
        POLICY_TESTS,
    ),
)
BROKER_MUTATIONS = (
    Mutation(
        "record_digest",
        RECORD,
        "self.action_id != action_digest(self.proposal)",
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "record_approval_state",
        RECORD,
        '(self.state == "PROPOSED") != (self.approval is None)',
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "record_two_person", RECORD, "approval.subject == self.proposer", "False", BROKER_TESTS
    ),
    Mutation(
        "record_approval_digest",
        RECORD,
        "approval.action_digest != self.action_id",
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "record_approval_lifetime",
        RECORD,
        "0 < lifetime <= 300",
        "0 < lifetime <= 301",
        BROKER_TESTS,
    ),
    Mutation(
        "record_effect_identity",
        RECORD,
        "(self.result.resource_uid, self.result.previous_version) != (\n"
        "                self.proposal.resource_uid,\n"
        "                self.proposal.expected_version,\n            )",
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "store_action_row_identity",
        STORE,
        "record.action_id != row.action_id",
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "store_audit_row_identity",
        STORE,
        "(event.action_id, event.revision) != (row.action_id, row.revision)",
        "False",
        BROKER_TESTS,
    ),
    Mutation(
        "store_claim_cas",
        STORE,
        "ActionRow.payload == before.model_dump_json(),",
        "True,",
        BROKER_TESTS,
    ),
    Mutation(
        "store_claim_audit",
        STORE,
        "            self._audit(session, after, actor, reason)",
        "            pass",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_identity_subject",
        BROKER,
        "or principal.subject != subject",
        "or False",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_worker_mode", BROKER, "record.proposal.mode != self.mode", "False", BROKER_TESTS
    ),
    Mutation(
        "broker_final_authority",
        BROKER,
        "            self._dispatch_authority(record, subject, policy_deadline)",
        "            pass",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_resource_deadline",
        BROKER,
        "context.resource.observed_at + timedelta(seconds=30)",
        "context.resource.observed_at + timedelta(seconds=300)",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_evidence_deadline",
        BROKER,
        "item.observed_at + timedelta(seconds=300)",
        "item.observed_at + timedelta(seconds=3600)",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_dispatch_deadline",
        BROKER,
        "if self.clock() > policy_deadline:",
        "if False:",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_approval_deadline", BROKER, "if now > policy_deadline:", "if False:", BROKER_TESTS
    ),
    Mutation(
        "broker_approval_time",
        BROKER,
        "if not approval.approved_at <= self.clock() < approval.expires_at:",
        "if False:",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_idempotency_key",
        BROKER,
        "self.backend.execute(record.proposal, record.action_id)",
        "self.backend.execute(record.proposal, record.proposer)",
        BROKER_TESTS,
    ),
    Mutation(
        "broker_duplicate_dispatch",
        BROKER,
        "            result = self.backend.execute(record.proposal, record.action_id)",
        "            self.backend.execute(record.proposal, record.action_id)\n"
        "            result = self.backend.execute(record.proposal, record.action_id)",
        BROKER_TESTS,
    ),
)
BUDGET_MUTATIONS = (
    *(
        Mutation(f"budget_{name}", BUDGET, predicate, "False", BUDGET_TESTS)
        for name, predicate in (
            ("model_calls", "len(model) > self.limits.model_calls"),
            ("tokens", "sum(charge.tokens() for charge in model) > self.limits.tokens"),
            ("cost", "sum(charge.cost() for charge in model) > self.limits.cost_nano_usd"),
            (
                "tool_calls",
                "sum(len(charge.requests) for charge in reads) > self.limits.tool_calls",
            ),
            (
                "backend_reads",
                "sum(charge.backend_reads() for charge in reads) > self.limits.backend_reads",
            ),
        )
    ),
    Mutation(
        "budget_output_tokens",
        BUDGET,
        "self.input_tokens + self.output_token_limit",
        "self.input_tokens",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_output_cost",
        BUDGET,
        "(self.output_token_limit * self.price.output_nano_usd)",
        "0",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_cache_cost",
        BUDGET,
        "max(\n            self.price.input_nano_usd, self.price.cached_input_nano_usd\n        )",
        "self.price.input_nano_usd",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_replay_dispatch",
        BUDGET,
        'raise BudgetConflict("budget changed before replay")\n            return "EXISTING"',
        'raise BudgetConflict("budget changed before replay")\n            return "NEW"',
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_operation_binding", BUDGET, "if existing != charge:", "if False:", BUDGET_TESTS
    ),
    Mutation(
        "budget_reopen_binding",
        BUDGET,
        "existing.binding_sha256 != binding_sha256",
        "False",
        BUDGET_TESTS,
    ),
    Mutation("budget_reopen_limits", BUDGET, "existing.limits != limits", "False", BUDGET_TESTS),
    Mutation(
        "budget_row_identity", BUDGET, "if record.run_id != row.run_id:", "if False:", BUDGET_TESTS
    ),
    Mutation(
        "budget_compare_and_set",
        BUDGET,
        "BudgetRow.payload == raw_payload,",
        "True,",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_nested_charge_validation",
        BUDGET,
        "        charge = CHARGE.validate_json(charge.model_dump_json())",
        "        pass",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_catalog_census",
        BUDGET,
        "self.backend_read_count != sum(\n"
        "            CATALOG[item.tool].backend_reads for item in self.requests\n        )",
        "False",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_stale_replay",
        BUDGET,
        "if self.get(expected.run_id) != expected:",
        "if False:",
        BUDGET_TESTS,
    ),
)
COMPLETION_MUTATIONS = (
    Mutation(
        "budget_completion_unique",
        BUDGET,
        "len(completed) != len(self.completions)",
        "False",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_completion_reserved",
        BUDGET,
        "not completed <= {\n            charge.operation_id for charge in self.charges\n        }",
        "False",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_completion_digest",
        BUDGET,
        "existing != receipt or self.get(expected.run_id) != expected",
        "self.get(expected.run_id) != expected",
        BUDGET_TESTS,
    ),
    Mutation(
        "budget_completion_replay",
        BUDGET,
        'raise BudgetConflict("completion differs or budget changed")\n'
        '            return "EXISTING"',
        'raise BudgetConflict("completion differs or budget changed")\n            return "NEW"',
        BUDGET_TESTS,
    ),
)
REGISTRY_MUTATIONS = (
    Mutation(
        "registry_service_scope",
        REGISTRY,
        "or item.resource != request.service",
        "",
        REGISTRY_TESTS,
    ),
    Mutation(
        "registry_publication_gate",
        REGISTRY,
        "return self._publish(results)",
        "return results",
        REGISTRY_TESTS,
    ),
    Mutation(
        "registry_publication_timeout",
        REGISTRY,
        'return status if completed <= deadline else "TIMEOUT"',
        "return status",
        REGISTRY_TESTS,
    ),
    Mutation(
        "registry_publication_discard",
        REGISTRY,
        'if item.status == "OK" and status != "OK"',
        "if False",
        REGISTRY_TESTS,
    ),
    Mutation(
        "registry_reservation_denial",
        REGISTRY,
        "if not self._reserve(validated, cost):",
        "if False:",
        REGISTRY_TESTS,
    ),
    Mutation(
        "registry_late_read",
        REGISTRY,
        "if result.completed_at > deadline:",
        "if False:",
        REGISTRY_TESTS,
    ),
)
GRAPH_BOUNDARY_MUTATIONS = (
    Mutation(
        "graph_backend_allowance",
        NODES,
        "if backend > state.budget.max_backend_reads:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_backend_dispatch_reservation",
        NODES,
        "or state.backend_reads_reserved < required_reads",
        "",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_payment_read_census",
        NODES,
        '34 if state.collection_profile == "payment_windows_v1" else 30\n            )',
        "30\n            )",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_resume_profile",
        GRAPH,
        "if existing.collection_profile != self.collection_profile:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_nested_payment",
        VERIFY,
        "            verify_payment_window(item, store)",
        "            pass",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_nested_trace",
        VERIFY,
        "            verify_trace_span(item, store)",
        "            pass",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_nested_retrieval",
        VERIFY,
        "            verify_retrieval_evidence(item, store)",
        "            pass",
        GRAPH_TESTS,
    ),
)
EXECUTOR_TESTS = "tests/unit/test_local_executor.py"
EXECUTOR_PLAN = "src/payops/remediation/deployment.py"
EXECUTOR_MUTATIONS = (
    Mutation(
        "executor_mode", EXECUTOR_PLAN, 'action.mode != "local_kind"', "False", EXECUTOR_TESTS
    ),
    Mutation(
        "executor_digest", EXECUTOR_PLAN, "key != action_digest(action)", "False", EXECUTOR_TESTS
    ),
    Mutation(
        "executor_uid_cas",
        EXECUTOR_PLAN,
        '{"op": "test", "path": "/metadata/uid", "value": self.action.resource_uid}',
        '{"op": "test", "path": "/metadata/name", "value": self.action.service}',
        EXECUTOR_TESTS,
    ),
    Mutation(
        "executor_ready_count",
        EXECUTOR_PLAN,
        'for field in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")',
        'for field in ("replicas", "updatedReplicas", "availableReplicas")',
        EXECUTOR_TESTS,
    ),
)
MUTATIONS = (
    CORE_MUTATIONS
    + POLICY_MUTATIONS
    + BROKER_MUTATIONS
    + BUDGET_MUTATIONS
    + COMPLETION_MUTATIONS
    + REGISTRY_MUTATIONS
    + GRAPH_BOUNDARY_MUTATIONS
    + EXECUTOR_MUTATIONS
)


def snapshot(repo: Path, destination: Path) -> dict[str, str]:
    """Copy only Python source/tests, excluding credentials, environments and cloud state."""
    hashes: dict[str, str] = {}
    for directory in ("src", "tests"):
        for source in sorted((repo / directory).rglob("*.py")):
            relative = source.relative_to(repo)
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            content = source.read_bytes()
            target.write_bytes(content)
            hashes[relative.as_posix()] = hashlib.sha256(content).hexdigest()
    (destination / "pytest.ini").write_text("[pytest]\naddopts = --strict-markers\n")
    return hashes


def classify(report: Path, returncode: int) -> tuple[str, list[dict[str, str]]]:
    """Only assertion failures or explicit pytest expectation failures count as killed."""
    if not report.exists():
        return "infrastructure_error", []
    try:
        root = ET.parse(report).getroot()
    except ET.ParseError:
        return "infrastructure_error", []
    errors = list(root.iter("error"))
    failures = list(root.iter("failure"))
    details = [
        {"type": node.get("type", ""), "message": node.get("message", "")}
        for node in errors + failures
    ]
    if errors:
        return "collection_or_setup_error", details
    if returncode == 0 and not failures:
        return "survived", details
    assertion_failures = [
        node
        for node in failures
        if node.get("message", "").startswith(
            ("AssertionError", "assert ", "Failed: DID NOT RAISE")
        )
    ]
    if returncode == 1 and assertion_failures:
        return "killed_by_assertion", details
    return "test_runtime_error", details


def run_tests(workspace: Path, selection: str) -> dict[str, Any]:
    """Fresh interpreters verify imports point into the snapshot before selected tests run."""
    keep = {"SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "TEMP", "TMP", "TMPDIR", "COMSPEC"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in keep}
    environment.update(
        PYTHONPATH=str(workspace / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
    )
    report = workspace / "junit.xml"
    probe = subprocess.run(
        [sys.executable, "-c", "import payops; print(payops.__file__)"],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if probe.returncode or str(workspace) not in probe.stdout:
        return {"status": "import_isolation_error", "import_probe": probe.stdout + probe.stderr}
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-c",
        "pytest.ini",
        "-q",
        "--tb=short",
        f"--junitxml={report}",
        *selection.split(),
    ]
    try:
        result = subprocess.run(
            command, cwd=workspace, env=environment, capture_output=True, text=True, timeout=60
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "command": command}
    (workspace / "pytest.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
    status, details = classify(report, result.returncode)
    return {
        "status": status,
        "returncode": result.returncode,
        "failures": details,
        "command": command,
        "import_probe": probe.stdout.strip(),
    }


def run_mutation(baseline: Path, output: Path, mutation: Mutation) -> dict[str, Any]:
    """Preserve each complete mutated snapshot and exact diff inputs for later review."""
    workspace = output / mutation.name
    shutil.copytree(baseline, workspace, ignore=shutil.ignore_patterns("junit.xml", "pytest.txt"))
    path = workspace / mutation.path
    original = path.read_text(encoding="utf-8")
    if original.count(mutation.before) != 1:
        return {"mutation": asdict(mutation), "status": "replacement_mismatch"}
    mutated = original.replace(mutation.before, mutation.after, 1)
    try:
        ast.parse(mutated)
    except SyntaxError as error:
        return {"mutation": asdict(mutation), "status": "invalid_syntax", "error": str(error)}
    path.write_text(mutated, encoding="utf-8")
    return {"mutation": asdict(mutation), **run_tests(workspace, mutation.test)}


def run_metadata(repo: Path, baseline: Path) -> dict[str, Any]:
    """Record actual source bytes and environment versions, including uncommitted inputs."""
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    return {
        "git_sha": revision,
        "git_status": subprocess.run(
            ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True
        ).stdout,
        "lock_sha256": hashlib.sha256((repo / "uv.lock").read_bytes()).hexdigest(),
        "source_hashes": snapshot(repo, baseline),
        "started_at": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "python_executable": sys.executable,
        "dependency_versions": {
            name: version(name)
            for name in ("pytest", "pydantic", "sqlalchemy", "langgraph", "opentelemetry-sdk")
        },
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "results": [],
    }


def main() -> int:
    """Non-killed mutants return a failing gate while retaining every result for diagnosis."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo, parent = args.repo.resolve(), args.output.resolve()
    if parent == repo or repo in parent.parents:
        parser.error("mutation evidence must be outside the source repository")
    parent.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="semantic-", dir=parent))
    baseline = output / "baseline"
    manifest = run_metadata(repo, baseline)
    selection = (
        f"{SCHEMA_TESTS} {EVIDENCE_TESTS} {METRIC_TESTS} "
        f"{GRAPH_TESTS} {POLICY_TESTS} {BROKER_TESTS} {BUDGET_TESTS} "
        f"{REGISTRY_TESTS} {EXECUTOR_TESTS}"
    )
    baseline_result = run_tests(baseline, selection)
    manifest["baseline"] = baseline_result
    if baseline_result["status"] != "survived":
        manifest["gate"] = "baseline_failed"
    else:
        for mutation in MUTATIONS:
            result = run_mutation(baseline, output, mutation)
            manifest["results"].append(result)
            print(f"{mutation.name}: {result['status']}", flush=True)
        manifest["gate"] = (
            "pass"
            if all(result["status"] == "killed_by_assertion" for result in manifest["results"])
            else "fail"
        )
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Evidence: {output}", flush=True)
    return 0 if manifest["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
