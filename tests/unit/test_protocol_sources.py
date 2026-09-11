"""Rehashed source substitutions cannot manufacture protocol control success."""

from datetime import timedelta
from pathlib import Path

import pytest
from test_protocol_observation import observed

from payops.evidence.trace_span import publish_trace_log, verify_trace_log
from payops.scenarios.protocol_observation import verify_protocol_observation


@pytest.mark.parametrize(
    "field", ["missing_peer", "parent", "error", "identity", "partial", "incident", "time"]
)
def test_rehashed_source_changes_cannot_fake_full_path(tmp_path: Path, field: str) -> None:
    """Each counterfactual republishes valid source bytes, isolating semantic verification."""
    item, store = observed(tmp_path)
    source = item.capture.sources[-1]
    log = verify_trace_log(source, store)
    first = log.parsed.spans[0]
    if field == "missing_peer":
        log = log.model_copy(update={"parsed": log.parsed.model_copy(update={"spans": ()})})
    elif field in {"parent", "error"}:
        span = first.span.model_copy(
            update={"parent_id": "0x" + "f" * 16} if field == "parent" else {"status_code": "ERROR"}
        )
        log = log.model_copy(
            update={
                "parsed": log.parsed.model_copy(
                    update={"spans": (first.model_copy(update={"span": span}),)}
                )
            }
        )
    elif field == "identity":
        log = log.model_copy(
            update={"identity": log.identity.model_copy(update={"pod_uid": "foreign"})}
        )
    elif field == "partial":
        log = log.model_copy(
            update={"parsed": log.parsed.model_copy(update={"partial_candidates": 1})}
        )
    elif field == "incident":
        log = log.model_copy(
            update={"scope": log.scope.model_copy(update={"incident_id": "other"})}
        )
    else:
        log = log.model_copy(
            update={
                "scope": log.scope.model_copy(update={"end": log.scope.end - timedelta(seconds=1)})
            }
        )
    replacement = publish_trace_log(log, store)
    verify_trace_log(replacement, store)
    capture = item.capture.model_copy(update={"sources": (*item.capture.sources[:-1], replacement)})
    with pytest.raises(ValueError):
        verify_protocol_observation("original", item.model_copy(update={"capture": capture}), store)


def test_negative_request_cannot_have_successful_risk_path(tmp_path: Path) -> None:
    """The exact 422 boundary cannot coexist with a claimed successful known risk SERVER."""
    item, store = observed(tmp_path, "mismatch")
    positive, other_store = observed(tmp_path / "positive")
    peer_log = verify_trace_log(positive.capture.sources[1], other_store)
    negative_log = verify_trace_log(item.capture.sources[1], store)
    row = peer_log.parsed.spans[0]
    row = row.model_copy(
        update={
            "span": row.span.model_copy(
                update={"trace_id": "0x" + item.probe.traceparent.split("-")[1]}
            )
        }
    )
    forged = negative_log.model_copy(
        update={"parsed": negative_log.parsed.model_copy(update={"spans": (row,)})}
    )
    replacement = publish_trace_log(forged, store)
    verify_trace_log(replacement, store)
    sources = list(item.capture.sources)
    sources[1] = replacement
    with pytest.raises(ValueError, match="path"):
        verify_protocol_observation(
            "mismatch",
            item.model_copy(
                update={"capture": item.capture.model_copy(update={"sources": tuple(sources)})}
            ),
            store,
        )
