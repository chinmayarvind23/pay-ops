import bundle from "./replay.json";

type ReplayCase = (typeof bundle.cases)[number];
type Evidence = ReplayCase["evidence"][number];

const causeNames: Record<string, string> = {
  STARTUP_FAILURE: "Application startup failure",
  INVALID_CONFIGURATION: "Invalid deployment configuration",
  READINESS_PROBE_FAILURE: "Readiness probe failure",
  PROCESSOR_UNAVAILABLE: "Processor unavailable",
};
const factNames: Record<string, string> = {
  replicas: "Desired replicas",
  generation: "Deployment generation",
  readyReplicas: "Ready replicas",
  availableReplicas: "Available replicas",
  updatedReplicas: "Updated replicas",
  pod_count: "Observed pods",
  running: "Pod phase: Running",
  ready: "Container ready",
  restartCount: "Container restarts",
  exitCode: "Previous exit code",
  crashLoopBackOff: "CrashLoopBackOff observed",
  readinessProbe404: "Readiness probe 404 observed",
  configurationValidationError: "Configuration validation error",
  invalidSyntheticOrigin: "Invalid service origin",
  http503Observed: "HTTP 503 observed",
};

/** Fail visibly if application markup and its typed renderer diverge. */
function node(id: string): HTMLElement {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing replay element: ${id}`);
  return element;
}

/** All source-derived values enter textContent; no replay value becomes HTML. */
function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className = "",
  text = "",
): HTMLElementTagNameMap[K] {
  const result = document.createElement(tag);
  result.className = className;
  result.textContent = text;
  return result;
}

/** Citation navigation preserves keyboard focus and respects reduced motion. */
function showEvidence(id: string): void {
  document
    .querySelectorAll(".evidence.highlight")
    .forEach((item) => item.classList.remove("highlight"));
  const target = node(`evidence-${id}`);
  target.classList.add("highlight");
  target.scrollIntoView({
    block: "nearest",
    behavior: matchMedia("(prefers-reduced-motion: reduce)").matches
      ? "instant"
      : "smooth",
  });
  target.focus({ preventScroll: true });
}

/** Keep the recorded order and confidence; the replay performs no new ranking. */
function renderCause(
  cause: ReplayCase["causes"][number],
  index: number,
): HTMLElement {
  const card = element("article", "cause");
  card.append(
    element("div", "rank", `RANK ${String(index + 1).padStart(2, "0")}`),
  );
  card.append(element("h4", "", causeNames[cause.cause] ?? cause.cause));
  const score = element("div", "score");
  score.append(
    element("span", "", "Recorded confidence"),
    element("strong", "", cause.confidence.toFixed(2)),
  );
  const track = element("div", "score-track");
  track.setAttribute("aria-hidden", "true");
  const bar = element("div");
  bar.style.width = `${cause.confidence * 100}%`;
  track.append(bar);
  card.append(score, track);
  for (const [label, ids] of [
    ["Supports", cause.supports],
    ["Refutes", cause.refutes],
  ] as const) {
    if (!ids.length) continue;
    const row = element("div", "citations");
    row.append(element("span", "", label));
    for (const id of ids) {
      const button = element("button", "cite", id);
      button.type = "button";
      button.setAttribute(
        "aria-label",
        `Inspect ${id}: ${label.toLowerCase()} rank ${index + 1}`,
      );
      button.addEventListener("click", () => showEvidence(id));
      row.append(button);
    }
    card.append(row);
  }
  return card;
}

/** Selected facts preserve absence: a missing counter is never rendered as zero. */
function renderEvidence(item: Evidence): HTMLElement {
  const card = element("article", "evidence");
  card.id = `evidence-${item.id}`;
  card.tabIndex = -1;
  card.setAttribute(
    "aria-label",
    `${item.id} ${item.source} evidence from ${item.service}`,
  );
  const heading = element("div", "evidence-head");
  heading.append(
    element("span", "evidence-id", item.id),
    element("span", "source", item.source),
    element("strong", "", item.service),
  );
  const facts = element("dl", "facts");
  for (const [key, value] of Object.entries(item.facts)) {
    if (value === undefined) continue;
    facts.append(
      element("dt", "", factNames[key] ?? key),
      element(
        "dd",
        "",
        typeof value === "boolean" ? (value ? "Yes" : "No") : String(value),
      ),
    );
  }
  const meta = element("div", "evidence-meta");
  const time = element(
    "time",
    "",
    new Intl.DateTimeFormat("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      timeZone: "UTC",
    }).format(new Date(item.observed_at)) + " UTC",
  );
  time.dateTime = item.observed_at;
  const digest = element("code", "", `SHA ${item.source_sha256.slice(0, 12)}…`);
  digest.title = item.source_sha256;
  meta.append(time, digest);
  card.append(heading, facts, meta);
  return card;
}

/** A case selection only replaces local replay content; it dispatches no requests. */
function render(selected: ReplayCase): void {
  document
    .querySelectorAll<HTMLButtonElement>(".case-button")
    .forEach((button) => {
      button.setAttribute(
        "aria-current",
        String(button.dataset.case === selected.case_id),
      );
    });
  node("case-label").textContent =
    `${selected.case_id} / RECORDED INVESTIGATION`;
  node("case-title").textContent = selected.title;
  node("duration").textContent = `${selected.duration_seconds.toFixed(2)} s`;
  node("evidence-count").textContent = String(selected.evidence_count);
  node("projected-count").textContent =
    `${selected.evidence.length} cited sources projected below`;
  node("outcome").textContent = selected.terminal_state
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/^./, (char) => char.toUpperCase());
  node("causes").replaceChildren(...selected.causes.map(renderCause));
  node("evidence").replaceChildren(...selected.evidence.map(renderEvidence));
  node("source-revision").textContent = bundle.source_revision;
  node("report-digest").textContent = selected.report_sha256;
  document.title = `${selected.case_id} · PayOps replay`;
}

/** Only recorded case identifiers are accepted from the address fragment. */
function selectedCase(): ReplayCase {
  return (
    bundle.cases.find((item) => `#${item.case_id}` === location.hash) ??
    bundle.cases[0]!
  );
}

for (const item of bundle.cases) {
  const button = element("button", "case-button");
  button.type = "button";
  button.dataset.case = item.case_id;
  button.append(
    element("span", "", item.case_id),
    element("strong", "", item.title),
  );
  button.addEventListener("click", () => {
    history.replaceState(null, "", `#${item.case_id}`);
    render(item);
  });
  node("case-list").append(button);
}
node("case-title").setAttribute("aria-live", "polite");
window.addEventListener("hashchange", () => render(selectedCase()));
render(selectedCase());
