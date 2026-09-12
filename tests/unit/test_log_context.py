"""Source projection preserves unparsed evidence and exposes structured mechanism fields."""

from payops.evidence.diagnostic_support import support_index
from payops.evidence.log_context import log_context


def test_structured_logs_retain_timestamp_and_ignore_archived_mechanism() -> None:
    """Real timestamp-prefixed JSON reaches predicates; an archived rival error does not."""
    projected = log_context(
        {
            "lines": "\n".join(
                [
                    '2026-09-12T00:00:00Z {"dependency":"redis","outcome":"unavailable"}',
                    '{"dependency":"postgres","sqlstate":"53300","archived":true}',
                    "unstructured configuration error",
                ]
            )
        }
    )
    assert projected["records"] == [
        {
            "record": {"dependency": "redis", "outcome": "unavailable"},
            "log_timestamp": "2026-09-12T00:00:00Z",
        },
        {
            "record": {"dependency": "postgres", "sqlstate": "53300", "archived": True},
            "log_timestamp": None,
        },
    ]
    assert projected["unparsed_lines"] == ["unstructured configuration error"]
    assert support_index((("source", "payments-api", projected),)) == {
        "CACHE_UNAVAILABLE": ("source",)
    }


def test_other_payloads_and_malformed_json_remain_evidence() -> None:
    """A parser failure cannot drop original text or turn arbitrary prose into a mechanism."""
    assert log_context({"text": "hello"}) == {"text": "hello"}
    assert log_context({"lines": 1}) == {"lines": 1}
    assert log_context({"lines": "{bad", "service": "payments-api"}) == {
        "service": "payments-api",
        "records": [],
        "unparsed_lines": ["{bad"],
        "projection": "structured_log_lines_v1",
    }
    for line in ('{"archived":true,"archived":false}', '{"value":NaN}', "[]"):
        assert log_context({"lines": line})["unparsed_lines"] == [line]
