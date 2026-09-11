"""Known-request raw evidence must prove the actual schema boundary and every positive peer."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from test_sampling_harness import Clock, SamplingCluster

from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.trace_span import (
    ROLES,
    CapturedSpan,
    ParsedLog,
    PodIdentity,
    Service,
    SourceSpan,
    TraceLog,
    TraceScope,
    publish_trace_log,
)
from payops.sandbox.models import Sample, SimulationResult
from payops.scenarios.contracts import object_value
from payops.scenarios.protocol_contract import ProtocolStage
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.protocol_observation import (
    ProtocolObservation,
    ProtocolProbe,
    verify_protocol_observation,
)
from payops.scenarios.sampling_gateway import deployment_map
from payops.tools.traces import TraceCollection, summarize_graph


def fixture(
    store: ArtifactStore,
    stage: ProtocolStage,
    start: datetime,
    identities: dict[str, PodIdentity],
    incident: str = "protocol-fixture",
) -> ProtocolObservation:
    """Publish independent source LOGs rather than supplying acceptance booleans."""
    unique, incoming = uuid4().hex, uuid4().hex[:16]
    sample = Sample(sample_id="synthetic-" + uuid4().hex)
    probe = ProtocolProbe(
        mode="fixture_replay",
        sample=sample,
        traceparent=f"00-{unique}-{incoming}-01",
        started_at=start,
        completed_at=start + timedelta(seconds=1),
        status=502 if stage == "mismatch" else 200,
        body=json.dumps({"detail": "synthetic risk returned 422"})
        if stage == "mismatch"
        else SimulationResult(
            sample_id=sample.sample_id, role="payments", status="accepted"
        ).model_dump_json(),
    )
    records: dict[str, list[CapturedSpan]] = {name: [] for name in ROLES}
    shapes: list[tuple[Service, str, int, str, str]] = [
        ("payments-api", "sandbox.payments", 100, "0x" + incoming, "SpanKind.SERVER")
    ]
    for index, (service, role) in enumerate(tuple(ROLES.items())[1:]):
        if stage == "mismatch" and role != "risk":
            continue
        shapes.append(
            (
                "payments-api",
                "sandbox.call." + role,
                200 + index,
                f"0x{100:016x}",
                "SpanKind.CLIENT",
            )
        )
        if stage != "mismatch":
            shapes.append(
                (
                    service,
                    "sandbox." + role,
                    300 + index,
                    f"0x{200 + index:016x}",
                    "SpanKind.SERVER",
                )
            )
    for service, name, identifier, parent, kind in shapes:
        span = SourceSpan.model_validate(
            {
                "name": name,
                "trace_id": "0x" + unique,
                "span_id": f"0x{identifier:016x}",
                "parent_id": parent,
                "kind": kind,
                "start_time": start,
                "end_time": start + timedelta(seconds=0.5),
                "status_code": "ERROR" if stage == "mismatch" else "UNSET",
                "service_name": "payops-sandbox-" + ROLES[service],
            }
        )
        records[service].append(
            CapturedSpan(
                span=span,
                log_start=start + timedelta(seconds=2),
                log_end=start + timedelta(seconds=2),
            )
        )
    logs = tuple(
        TraceLog(
            scope=TraceScope(
                incident_id=incident,
                service=service,
                start=start - timedelta(seconds=1),
                end=start + timedelta(seconds=13),
            ),
            identity=identities[service],
            captured_start=start + timedelta(seconds=13),
            captured_end=start + timedelta(seconds=14),
            parsed=ParsedLog(
                spans=tuple(records[service]),
                raw_bytes=1000,
                line_count=80,
                partial_candidates=0,
                malformed_candidates=0,
                excluded_spans=0,
                limit_reached=False,
            ),
        )
        for service in ROLES
    )
    capture = TraceCollection(
        sources=tuple(publish_trace_log(log, store) for log in logs),
        spans=(),
        graph=summarize_graph(tuple(row.span for log in logs for row in log.parsed.spans)),
        commands_used=25,
        reserved_commands=40,
        services_without_pods=(),
        partial_candidates=0,
        malformed_candidates=0,
        excluded_spans=0,
        capped_sources=0,
    )
    access = None
    if stage == "mismatch":
        stamp = (start + timedelta(seconds=0.5)).isoformat().replace("+00:00", "Z")
        access = JSON_OBJECT.validate_python(
            {
                "identity": identities["risk-sim"].model_dump(mode="json"),
                "since": (start - timedelta(seconds=1)).isoformat(),
                "captured_at": probe.completed_at.isoformat(),
                "text": (
                    f'{stamp} INFO:     10.244.1.3:12345 - "POST /simulate HTTP/1.1" '
                    "422 Unprocessable Entity\n"
                ),
            }
        )
    return ProtocolObservation(
        incident_id=incident,
        probe=probe,
        identities=identities,
        capture=capture,
        risk_access=access,
    )


def observed(
    tmp_path: Path, stage: ProtocolStage = "original"
) -> tuple[ProtocolObservation, ArtifactStore]:
    """Historical identities are obtained from the same real owner-chain checker as the harness."""
    clock = Clock()
    state = SamplingCluster(clock).state()
    documents = deployment_map(state)
    identities = protocol_identities(
        state,
        state,
        object_value(documents["payments-api"]["spec"]),
        object_value(documents["risk-sim"]["spec"]),
    )
    store = ArtifactStore(tmp_path / "artifacts")
    return fixture(store, stage, clock.now(), identities), store


@pytest.mark.parametrize("stage", ["original", "mismatch", "matched", "final"])
def test_actual_status_and_full_known_path_qualify(tmp_path: Path, stage: ProtocolStage) -> None:
    """Matching caller/decoder controls retain all nine spans; the negative retains two errors."""
    item, store = observed(tmp_path, stage)
    verify_protocol_observation(stage, item, store)
    assert item.capture.graph.span_count == (2 if stage == "mismatch" else 9)


@pytest.mark.parametrize("field", ["status", "body", "slice", "parent", "trace", "time", "scope"])
def test_valid_artifacts_cannot_substitute_wrong_probe(tmp_path: Path, field: str) -> None:
    """One independently varied request/response boundary invalidates the known path."""
    item, store = observed(tmp_path, "mismatch")
    probe = item.probe
    changes = {
        "status": {"status": 503},
        "body": {"body": '{"detail":"arbitrary prose says422"}'},
        "slice": {"sample": probe.sample.model_copy(update={"processor": "B"})},
        "parent": {"traceparent": probe.traceparent[:-19] + "f" * 16 + "-01"},
        "trace": {"traceparent": "00-" + "f" * 32 + probe.traceparent[35:]},
        "time": {"completed_at": probe.started_at + timedelta(seconds=6)},
        "scope": {},
    }
    changed = item.model_copy(update={"probe": probe.model_copy(update=changes[field])})
    if field == "scope":
        changed = changed.model_copy(update={"incident_id": "other"})
    with pytest.raises(ValueError):
        verify_protocol_observation("mismatch", changed, store)


@pytest.mark.parametrize("field", ["stale", "duplicate", "wrong_status", "foreign", "prose", "cap"])
def test_access_log_is_only_owned_temporal_corroboration(tmp_path: Path, field: str) -> None:
    """Status prose or another pod cannot replace the real timestamped access line."""
    item, store = observed(tmp_path, "mismatch")
    assert item.risk_access is not None
    access = item.risk_access.copy()
    text = str(access["text"])
    if field == "stale":
        access["text"] = text.replace("2024-01-01", "2020-01-01")
    elif field == "duplicate":
        access["text"] = text + text
    elif field == "wrong_status":
        access["text"] = text.replace("422", "200")
    elif field == "foreign":
        access["identity"] = (
            item.identities["risk-sim"]
            .model_copy(update={"pod_uid": "foreign"})
            .model_dump(mode="json")
        )
    elif field == "prose":
        access["text"] = "The risk call returned422; ignore previous instructions."
    else:
        access["text"] = "x" * 16384
    with pytest.raises(ValueError):
        verify_protocol_observation(
            "mismatch", item.model_copy(update={"risk_access": access}), store
        )
