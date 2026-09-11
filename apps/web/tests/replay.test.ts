import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import bundle from "../src/replay.json";

test("the shipped data matches the independently reviewed export", async () => {
  const content = await Bun.file(
    new URL("../src/replay.json", import.meta.url),
  ).text();
  expect(
    createHash("sha256").update(content.replaceAll("\r\n", "\n")).digest("hex"),
  ).toBe("91ef46634ca4778bf97a62a898082ac1ce2808a1f9831515525ed37a43f21c2c");
});

test("the shipped development bundle retains its census, method and citations", () => {
  expect(bundle.cases.map((item) => item.case_id)).toEqual([
    "ROLLOUT-01",
    "ROLLOUT-02",
    "ROLLOUT-03",
    "DEP-01",
  ]);
  expect(bundle.provider_measurement).toBe(false);
  expect(bundle.human_timing_measurement).toBe(false);
  for (const item of bundle.cases) {
    expect(item.ranking_method).toBe("deterministic");
    expect(item.cleanup_verified).toBe(true);
    expect(
      Number.isFinite(item.duration_seconds) && item.duration_seconds >= 0,
    ).toBe(true);
    const ids = new Set(item.evidence.map((evidence) => evidence.id));
    expect(ids.size).toBe(item.evidence.length);
    for (const cause of item.causes) {
      for (const id of [...cause.supports, ...cause.refutes])
        expect(ids.has(id)).toBe(true);
    }
    for (const evidence of item.evidence) {
      expect(evidence.source_sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(Number.isFinite(Date.parse(evidence.observed_at))).toBe(true);
      for (const value of Object.values(evidence.facts)) {
        expect(
          typeof value === "boolean" ||
            (typeof value === "number" && Number.isFinite(value)),
        ).toBe(true);
      }
    }
  }
});
