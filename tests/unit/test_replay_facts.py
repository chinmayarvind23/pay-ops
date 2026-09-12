"""Offline evaluation must bind source bytes and keep scorer metadata out of model context."""

import json
from hashlib import sha256
from pathlib import Path

import pytest

from payops.evaluation.labels import EXPECTED
from payops.evaluation.replay_facts import (
    FactReference,
    ReplayCase,
    at_pointer,
    load_cases,
    prompt_data,
    read_fact,
)


def reference(root: Path, value: object) -> FactReference:
    """Use real local source bytes so integrity tests exercise hashing and projection."""
    raw = json.dumps({"observation": value}).encode()
    (root / "source.json").write_bytes(raw)
    return FactReference(
        evidence_id="e1",
        source="logs",
        path="source.json",
        sha256=sha256(raw).hexdigest(),
        pointer="/observation",
    )


def test_context_excludes_scorer_metadata(tmp_path: Path) -> None:
    """Neither accepted-cause annotation nor case ID may leak through prompt serialization."""
    ref = reference(tmp_path, {"reason": "OOMKilled"})
    case = ReplayCase(case_id="OOM-01", facts=(ref,), supporting_facts={"private-gold": ("e1",)})
    data, ids = prompt_data(tmp_path, case, frozenset({"CAUSE_A", "CAUSE_B"}))
    assert ids == {"e1"}
    assert "OOM-01" not in data and "private-gold" not in data and "source.json" not in data
    assert json.loads(data)["untrusted_observations"][0]["value"] == {"reason": "OOMKilled"}


def test_tampering_fails_before_projection(tmp_path: Path) -> None:
    """A plausible replacement fact cannot reuse an earlier file's checksum."""
    ref = reference(tmp_path, "first")
    (tmp_path / "source.json").write_text('{"observation":"replacement"}')
    with pytest.raises(ValueError, match="checksum"):
        read_fact(tmp_path, ref)


def test_source_cannot_escape_root(tmp_path: Path) -> None:
    """Relative traversal into an existing external file is still prohibited."""
    inner = tmp_path / "inside"
    inner.mkdir()
    ref = reference(tmp_path, "outside")
    ref = ref.model_copy(update={"path": "../source.json"})
    with pytest.raises(ValueError, match="escapes"):
        read_fact(inner, ref)


@pytest.mark.parametrize("pointer", ["a", "/a~2", "/a/01", "/a/-1", "/a/x"])
def test_invalid_pointer(pointer: str) -> None:
    """Pointer parsing accepts no query language or ambiguous array-index syntax."""
    with pytest.raises((ValueError, KeyError)):
        at_pointer({"a": [1]}, pointer)


def test_escaped_pointer_and_root() -> None:
    """Standard slash and tilde escapes resolve to literal source keys."""
    assert at_pointer({"a/b": {"~": [7]}}, "/a~1b/~0/0") == 7
    assert at_pointer(3, "") == 3


def test_line_and_nonzero_projection(tmp_path: Path) -> None:
    """Mechanical selection keeps exact source values rather than curator-written summaries."""
    ref = reference(tmp_path, "first\nsecond")
    assert read_fact(tmp_path, ref.model_copy(update={"line": 1})) == "second"
    ref = reference(tmp_path, [{"value": 0}, {"value": 2}])
    assert read_fact(tmp_path, ref.model_copy(update={"transform": "nonzero_metrics"})) == [
        {"value": 2}
    ]
    with pytest.raises(ValueError, match="text"):
        read_fact(tmp_path, ref.model_copy(update={"line": 1}))


@pytest.mark.parametrize("value", ["text", [{"value": True}], [{"value": "1"}]])
def test_non_numeric_metrics_rejected(tmp_path: Path, value: object) -> None:
    """Boolean and string counters cannot masquerade as numerical observations."""
    ref = reference(tmp_path, value).model_copy(update={"transform": "nonzero_metrics"})
    with pytest.raises(ValueError, match="metric rows"):
        read_fact(tmp_path, ref)


@pytest.mark.parametrize("value", ["OOM-01", "SANDBOX_INJECT_FAILURE", "x" * 14000])
def test_experiment_leakage_and_size_rejected(tmp_path: Path, value: str) -> None:
    """Context must omit experiment labels and fit the reviewed model-input bound."""
    case = ReplayCase(case_id="OOM-01", facts=(reference(tmp_path, value),))
    with pytest.raises(ValueError):
        prompt_data(tmp_path, case, frozenset({"A"}))


def test_annotation_and_duplicate_ids_rejected(tmp_path: Path) -> None:
    """Scorer annotations may reference only submitted facts and IDs must be unique."""
    ref = reference(tmp_path, "ok")
    for case in (
        ReplayCase(case_id="OOM-01", facts=(ref, ref)),
        ReplayCase(case_id="OOM-01", facts=(ref,), supporting_facts={"A": ("absent",)}),
    ):
        with pytest.raises(ValueError):
            prompt_data(tmp_path, case, frozenset({"A"}))


def test_all_cases_required_before_dispatch(tmp_path: Path) -> None:
    """Missing and duplicated cases cannot improve recall by shrinking its denominator."""
    ref = reference(tmp_path, "ok").model_dump(mode="json")
    cases = [{"case_id": case, "facts": [ref]} for case in sorted(EXPECTED)]
    assert len(load_cases(json.dumps({"version": 1, "cases": cases}).encode())) == 24
    for bad in (cases[:-1], cases + [cases[0]], cases[:-1] + [cases[0]]):
        with pytest.raises(ValueError):
            load_cases(json.dumps({"version": 1, "cases": bad}).encode())
    with pytest.raises(ValueError):
        load_cases(b'{"version":2,"cases":[]}')
