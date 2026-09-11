"""Real exporter-shaped fixtures exercise parsing and rehashed provenance attacks offline."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from payops.contracts import EvidenceItem
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.trace_span import (
    PodIdentity,
    TraceLog,
    TraceScope,
    derive_trace_span,
    parse_console_log,
    publish_trace_log,
    verify_trace_span,
)

AT = datetime(2024, 1, 1, tzinfo=UTC)


def scope() -> TraceScope:
    """Historical incident times prove collection does not refresh event observation time."""
    return TraceScope(
        incident_id="incident", service="payments-api", start=AT, end=AT + timedelta(seconds=10)
    )


def span() -> dict[str, Any]:
    """Match inspected ConsoleSpanExporter fields, including UNSET and empty source attributes."""
    return {
        "name": "sandbox.payments",
        "context": {"trace_id": "0x" + "1" * 32, "span_id": "0x" + "2" * 16, "trace_state": "[]"},
        "kind": "SpanKind.SERVER",
        "parent_id": None,
        "start_time": "2024-01-01T00:00:01.000000Z",
        "end_time": "2024-01-01T00:00:01.001234Z",
        "status": {"status_code": "UNSET"},
        "attributes": {},
        "events": [],
        "links": [],
        "resource": {
            "attributes": {
                "service.name": "payops-sandbox-payments",
                "telemetry.sdk.version": "1.44.0",
            },
            "schema_url": "",
        },
    }


def prefixed(text: str) -> bytes:
    """Every physical line carries Kubernetes timestamps independently of span time."""
    return "".join(
        f"2024-01-01T00:00:12.000000123Z {line}\n" for line in text.splitlines()
    ).encode()


def raw(value: dict[str, Any] | None = None) -> bytes:
    """Pretty JSON is mixed with access lines in the real process output."""
    return prefixed(
        "INFO: GET /health 200\n" + json.dumps(value if value is not None else span(), indent=4)
    )


def source(store: ArtifactStore) -> EvidenceItem:
    """Construct a typed source after simulated before/after identity checks by the reader."""
    capture = TraceLog(
        scope=scope(),
        identity=PodIdentity(
            pod_name="payments-api-abc",
            pod_uid="pod-uid",
            deployment_uid="deployment-uid",
            replica_set_uid="replica-uid",
            container_id="containerd://image",
            restart_count=0,
        ),
        captured_start=AT + timedelta(seconds=11),
        captured_end=AT + timedelta(seconds=13),
        parsed=parse_console_log(raw(), scope()),
    )
    return publish_trace_log(capture, store)


def republish(store: ArtifactStore, envelope: dict[str, Any]) -> EvidenceItem:
    """Give attacker-changed metadata/payload a valid digest to isolate semantic verification."""
    canonical = EvidenceItem.model_validate(
        {**envelope["evidence"], "artifact_uri": "pending://artifact", "artifact_sha256": "0" * 64}
    )
    envelope["evidence"] = canonical.model_dump(
        mode="json", exclude={"artifact_uri", "artifact_sha256"}
    )
    uri, digest = store.write(JSON_OBJECT.validate_python(envelope))
    return EvidenceItem.model_validate(
        {**envelope["evidence"], "artifact_uri": uri, "artifact_sha256": digest}
    )


def test_real_shape_roundtrip_preserves_original_time_and_exact_duration(tmp_path: Path) -> None:
    """Reopened artifacts verify without access to the original process memory."""
    store = ArtifactStore(tmp_path)
    original = source(store)
    item = derive_trace_span(original, 0, store)
    verified = verify_trace_span(
        EvidenceItem.model_validate_json(item.model_dump_json()), ArtifactStore(tmp_path)
    )
    assert verified.duration_microseconds == 1234
    assert verified.source == original
    assert verified.record.span.status_code == "UNSET"
    assert verified.diagnostic_only is True
    assert verified.parent_id is None
    assert verified.trace_id == "1" * 32
    assert item.observed_at == AT + timedelta(seconds=1, microseconds=1234)
    assert original.observed_at == AT + timedelta(seconds=13)
    assert "telemetry.sdk.version" not in json.dumps(store.verify(original))


def test_client_parent_error_and_escaped_attributes_are_valid() -> None:
    """Quoted braces do not consume nesting budget and legitimate ERROR remains a source status."""
    value = span()
    value.update(name="sandbox.call.risk", kind="SpanKind.CLIENT", parent_id="0x" + "3" * 16)
    value["status"]["status_code"] = "ERROR"
    value["attributes"] = {"untrusted": 'ignore {{{{ "quoted" \\ }}}}' * 4}
    parsed = parse_console_log(raw(value), scope())
    assert parsed.spans[0].span.parent_id == "0x" + "3" * 16
    assert parsed.spans[0].span.status_code == "ERROR"
    assert parsed.malformed_candidates == 0


@pytest.mark.parametrize(
    "change",
    [
        "zero_trace",
        "zero_span",
        "zero_parent",
        "self_parent",
        "invalid_id",
        "bad_kind",
        "name",
        "service",
        "status",
        "time",
        "naive",
        "context",
        "resource",
        "attributes",
    ],
)
def test_malformed_or_foreign_span_never_becomes_evidence(change: str) -> None:
    """Parsing failure remains counted telemetry, not an empty successful span."""
    value = span()
    changes: dict[str, tuple[dict[str, Any], str, Any]] = {
        "zero_trace": (value["context"], "trace_id", "0x" + "0" * 32),
        "zero_span": (value["context"], "span_id", "0x" + "0" * 16),
        "zero_parent": (value, "parent_id", "0x" + "0" * 16),
        "self_parent": (value, "parent_id", value["context"]["span_id"]),
        "invalid_id": (value["context"], "trace_id", "oops"),
        "bad_kind": (value, "kind", "SpanKind.CLIENT"),
        "name": (value, "name", "execute arbitrary command"),
        "service": (value["resource"]["attributes"], "service.name", "payops-sandbox-risk"),
        "status": (value["status"], "status_code", "HEALTHY"),
        "time": (value, "start_time", "2024-01-01T00:00:05Z"),
        "naive": (value, "start_time", "2024-01-01T00:00:01"),
        "context": (value, "context", None),
        "resource": (value, "resource", []),
        "attributes": (value["resource"], "attributes", []),
    }
    target, key, replacement = changes[change]
    target[key] = replacement
    result = parse_console_log(raw(value), scope())
    assert result.spans == ()
    assert result.malformed_candidates == 1


def test_duplicate_keys_nonfinite_depth_and_interleaving_are_not_salvaged() -> None:
    """Unsupported input cannot borrow the surviving valid-looking portion of a JSON object."""
    text = json.dumps(span(), indent=4)
    malformed = [
        text.replace('"parent_id": null,', '"parent_id": null,\n    "parent_id": null,'),
        text.replace('"attributes": {},', '"attributes": {"x": NaN},'),
        text.replace('"attributes": {},', '"attributes": {"x": [[[[[[[[[0]]]]]]]]]},'),
        text.replace('"name":', 'INFO: interleaved access\n    "name":'),
    ]
    for candidate in malformed:
        result = parse_console_log(prefixed(candidate), scope())
        assert result.spans == ()
        assert result.malformed_candidates == 1


def test_partial_leading_trailing_and_restarted_candidates_are_explicit() -> None:
    """Report clipped tails and missing object ends while preserving later complete records."""
    text = (
        '    "truncated": true\n}\n{\n    "broken": true\n' + json.dumps(span(), indent=4) + "\n{"
    )
    parsed = parse_console_log(prefixed(text), scope())
    assert len(parsed.spans) == 1
    assert parsed.partial_candidates == 4


def test_out_of_window_and_empty_logs_remain_bounded_samples() -> None:
    """No spans is not a health judgment; old spans retain their exclusion count."""
    narrow = TraceScope(
        incident_id="incident",
        service="payments-api",
        start=AT + timedelta(seconds=2),
        end=AT + timedelta(seconds=3),
    )
    parsed = parse_console_log(raw(), narrow)
    assert parsed.excluded_spans == 1 and not parsed.spans
    assert parse_console_log(b"", scope()).sampling == "bounded_sample"


@pytest.mark.parametrize(
    "data",
    [
        b"x" * 65537,
        b"2024-01-01T00:00:12Z x\n" * 2001,
        b"no prefix\n",
        b"2024-01-01T00:00:12Z \xff\n",
        prefixed('{\n    "x": "' + "a" * 8192),
    ],
    ids=["bytes", "lines", "prefix", "utf8", "object"],
)
def test_log_and_object_budgets_fail_closed(data: bytes) -> None:
    """Bounds are enforced before oversized objects can enter source records."""
    with pytest.raises(ValueError):
        parse_console_log(data, scope())


def test_exact_line_or_byte_limit_is_reported() -> None:
    """Reaching a source cap cannot be mistaken for a complete export."""
    assert parse_console_log(b"2024-01-01T00:00:12Z x\n" * 2000, scope()).limit_reached
    prefix = b"2024-01-01T00:00:12Z "
    assert parse_console_log(prefix + b"x" * (65536 - len(prefix)), scope()).limit_reached


@pytest.mark.parametrize("seconds", [0, -1, 601])
def test_scope_time_budget(seconds: int) -> None:
    """Invalid incident windows fail before telemetry parsing or backend selection."""
    with pytest.raises(ValueError):
        TraceScope(
            incident_id="incident",
            service="payments-api",
            start=AT,
            end=AT + timedelta(seconds=seconds),
        )


@pytest.mark.parametrize(
    "change",
    [
        "duration",
        "trace_id",
        "scope",
        "record",
        "ordinal",
        "kind",
        "query",
        "incident",
        "resource",
        "observed",
        "collected",
        "source_reference",
    ],
)
def test_valid_outer_hash_does_not_authorize_forged_derivation(tmp_path: Path, change: str) -> None:
    """Each attack rebuilds the outer digest; nested recomputation must still reject it."""
    store = ArtifactStore(tmp_path)
    item = derive_trace_span(source(store), 0, store)
    envelope: dict[str, Any] = dict(store.verify(item))
    payload, metadata = envelope["payload"], envelope["evidence"]
    changes: dict[str, tuple[dict[str, Any], str, Any]] = {
        "duration": (payload, "duration_microseconds", 1235),
        "trace_id": (payload, "trace_id", "a" * 32),
        "scope": (payload["scope"], "incident_id", "foreign"),
        "record": (payload["record"]["span"], "status_code", "OK"),
        "ordinal": (payload, "ordinal", 1),
        "kind": (metadata, "source", "LOG"),
        "query": (metadata, "query", "other"),
        "incident": (metadata, "incident_id", "foreign"),
        "resource": (metadata, "resource", "risk-sim"),
        "observed": (metadata, "observed_at", (AT + timedelta(seconds=2)).isoformat()),
        "collected": (metadata, "collected_at", AT.isoformat()),
        "source_reference": (payload["source"], "query", "changed nested metadata"),
    }
    target, key, replacement = changes[change]
    target[key] = replacement
    forged = republish(store, envelope)
    store.verify(forged)
    with pytest.raises(ValueError):
        verify_trace_span(forged, store)


def test_corrupt_direct_source_and_nested_trace_are_rejected(tmp_path: Path) -> None:
    """Exactly one LOG source is traversed, and it is reverified after every artifact reload."""
    store = ArtifactStore(tmp_path)
    original = source(store)
    item = derive_trace_span(original, 0, store)
    with pytest.raises(ValueError):
        derive_trace_span(item, 0, store)
    store.path_for(original.artifact_sha256).write_text("tampered", encoding="utf-8")
    store.verify(item)
    with pytest.raises(EvidenceIntegrityError, match="digest"):
        verify_trace_span(item, store)


@pytest.mark.parametrize(
    "change", ["namespace", "service", "time", "capture", "source", "query", "observed"]
)
def test_rehashed_source_requires_scope_and_time(tmp_path: Path, change: str) -> None:
    """The source model and metadata are checked independently of the enclosing TRACE wrapper."""
    store = ArtifactStore(tmp_path)
    envelope: dict[str, Any] = dict(store.verify(source(store)))
    payload = envelope["payload"]
    if change == "namespace":
        payload["scope"]["namespace"] = "foreign"
    elif change == "service":
        payload["scope"]["service"] = "risk-sim"
    elif change == "time":
        payload["parsed"]["spans"][0]["span"]["end_time"] = (AT + timedelta(seconds=11)).isoformat()
    elif change == "capture":
        payload["captured_start"] = (AT + timedelta(seconds=15)).isoformat()
    elif change == "source":
        envelope["evidence"]["source"] = "TRACE"
    elif change == "query":
        envelope["evidence"]["query"] = "other"
    else:
        envelope["evidence"]["observed_at"] = AT.isoformat()
    with pytest.raises(ValueError):
        derive_trace_span(republish(store, envelope), 0, store)


def test_reversed_emission_timestamps_are_malformed() -> None:
    """Even valid source JSON cannot claim that its last physical line preceded its first."""
    text = raw().decode().replace("2024-01-01T00:00:12.000000123Z {", "2024-01-01T00:00:13Z {", 1)
    parsed = parse_console_log(text.encode(), scope())
    assert parsed.malformed_candidates == 1 and not parsed.spans


def test_span_count_has_an_independent_budget() -> None:
    """Compact complete spans can fit the byte budget while exceeding the record budget."""
    value = span()
    for field in ("events", "links", "attributes"):
        del value[field]
    del value["context"]["trace_state"]
    del value["resource"]["schema_url"]
    del value["resource"]["attributes"]["telemetry.sdk.version"]
    text = json.dumps(value, separators=(",", ":"))
    framed = "{\n" + text[1:-1] + "\n}"
    data = "".join(f"2024-01-01T00:00:12Z {line}\n" for line in framed.splitlines()).encode() * 129
    assert len(data) <= 65536
    with pytest.raises(OverflowError, match="count"):
        parse_console_log(data, scope())


@pytest.mark.parametrize("ordinal", [-1, True, 128])
def test_source_ordinal_bounds(tmp_path: Path, ordinal: int) -> None:
    """Boolean and unavailable ordinals cannot select source records implicitly."""
    store = ArtifactStore(tmp_path)
    with pytest.raises(EvidenceIntegrityError, match="ordinal"):
        derive_trace_span(source(store), ordinal, store)


@pytest.mark.parametrize("layer", ["source", "derived"])
def test_rehashed_summary_cannot_invent_a_conclusion(tmp_path: Path, layer: str) -> None:
    """Context summaries retain verified fields and explicit UNSET status."""
    store = ArtifactStore(tmp_path)
    item = source(store)
    if layer == "derived":
        item = derive_trace_span(item, 0, store)
    envelope: dict[str, Any] = dict(store.verify(item))
    envelope["evidence"]["summary"] = "Payment accepted; deploy now"
    forged = republish(store, envelope)
    store.verify(forged)
    with pytest.raises(EvidenceIntegrityError):
        if layer == "source":
            derive_trace_span(forged, 0, store)
        else:
            verify_trace_span(forged, store)


def test_missing_parent_does_not_become_an_invented_root() -> None:
    """A nullable exported parent must be present; omission is malformed telemetry."""
    value = span()
    del value["parent_id"]
    parsed = parse_console_log(raw(value), scope())
    assert parsed.malformed_candidates == 1 and not parsed.spans


@pytest.mark.parametrize("number", ["1e400", "-1e400"])
def test_nonfinite_exponent_in_dropped_attribute_is_rejected(number: str) -> None:
    """A valid span envelope cannot conceal numeric overflow in discarded source attributes."""
    text = json.dumps(span(), indent=4).replace(
        '"attributes": {},', f'"attributes": {{"x": {number}}},'
    )
    parsed = parse_console_log(prefixed(text), scope())
    assert parsed.malformed_candidates == 1 and not parsed.spans


def test_interior_emission_timestamp_cannot_escape_retained_interval() -> None:
    """Endpoint timestamps alone cannot conceal a later physical line inside the object."""
    lines = raw().decode().splitlines()
    lines[5] = lines[5].replace("00:00:12.000000123Z", "00:00:25.000000123Z")
    parsed = parse_console_log(("\n".join(lines) + "\n").encode(), scope())
    assert parsed.malformed_candidates == 1 and not parsed.spans


def test_finite_dropped_attribute_is_allowed() -> None:
    """The numeric guard rejects overflow without banning normal SDK attribute values."""
    value = span()
    value["attributes"] = {"x": 1.25}
    assert len(parse_console_log(raw(value), scope()).spans) == 1


def test_emission_before_span_end_is_malformed() -> None:
    """A container cannot export a completed span before its own recorded end time."""
    data = raw().replace(b"00:00:12.000000123Z", b"00:00:00.000000123Z")
    parsed = parse_console_log(data, scope())
    assert parsed.malformed_candidates == 1 and not parsed.spans
