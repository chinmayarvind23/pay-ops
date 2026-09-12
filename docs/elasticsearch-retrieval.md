# Elasticsearch Retrieval

Corpus:

- runbooks,
- postmortems,
- prior incident summaries,
- selected normalized evidence.

Operational queries mix exact identifiers and semantic intent, so the retrieval experiment compares lexical, semantic, and hybrid retrieval.

```text
BM25/full-text + semantic/vector
             |
             v
             RRF
             |
       optional rerank
```

RRF:

`RRF(d) = sum(1 / (k + rank_r(d)))`

Configuration is versioned with the retrieval contract.

Every retrieved item preserves document ID, version, source, section, timestamp, and retrieval rank/score.

Elasticsearch is read-only derived state, not current operational truth.
