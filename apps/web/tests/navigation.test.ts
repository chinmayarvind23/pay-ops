import { expect, test } from "bun:test";

/** A renderer fixture implements used DOM methods; it does not simulate layout or a browser. */
class ElementFixture {
  id = "";
  className = "";
  textContent = "";
  dataset: Record<string, string> = {};
  style: Record<string, string> = {};
  attributes = new Map<string, string>();
  events = new Map<string, () => void>();
  children: ElementFixture[] = [];
  classList = {
    add: (name: string) => {
      this.className += ` ${name}`;
    },
    remove: (name: string) => {
      this.className = this.className
        .split(" ")
        .filter((item) => item !== name)
        .join(" ");
    },
  };
  setAttribute(name: string, value: string): void {
    this.attributes.set(name, value);
  }
  append(...children: ElementFixture[]): void {
    this.children.push(...children);
  }
  replaceChildren(...children: ElementFixture[]): void {
    this.children = children;
  }
  addEventListener(name: string, callback: () => void): void {
    this.events.set(name, callback);
  }
}

test("actual renderer preserves the selected incident through skip and unrelated fragments", async () => {
  const elements: ElementFixture[] = [];
  const create = (): ElementFixture => {
    const item = new ElementFixture();
    elements.push(item);
    return item;
  };
  const ids = [
    "case-list",
    "case-title",
    "case-label",
    "duration",
    "evidence-count",
    "projected-count",
    "outcome",
    "causes",
    "evidence",
    "source-revision",
    "report-digest",
  ];
  for (const id of ids) create().id = id;
  const byId = (id: string): ElementFixture =>
    [...elements].reverse().find((item) => item.id === id)!;
  const events = new Map<string, () => void>();
  const location = { hash: "#unknown-initial-fragment" };
  const globals = {
    document: {
      getElementById: byId,
      createElement: create,
      querySelectorAll: (selector: string) =>
        elements.filter((item) =>
          selector
            .slice(1)
            .split(".")
            .every((name) => item.className.split(" ").includes(name)),
        ),
      title: "",
    },
    location,
    history: {
      replaceState: (_state: unknown, _unused: string, url: string) => {
        location.hash = url;
      },
    },
    window: {
      addEventListener: (name: string, callback: () => void) => {
        events.set(name, callback);
      },
    },
  };
  const saved = Object.keys(globals).map(
    (name) =>
      [name, Object.getOwnPropertyDescriptor(globalThis, name)] as const,
  );
  try {
    for (const [name, value] of Object.entries(globals)) {
      Object.defineProperty(globalThis, name, {
        value,
        configurable: true,
        writable: true,
      });
    }
    await import("../src/main");
    expect(byId("case-label").textContent).toStartWith("ROLLOUT-01");
    const chosen = byId("case-list").children.find(
      (item) => item.dataset.case === "DEP-01",
    )!;
    chosen.events.get("click")!();
    expect(location.hash).toBe("#DEP-01");
    expect(byId("case-label").textContent).toStartWith("DEP-01");
    expect(chosen.attributes.get("aria-current")).toBe("true");
    const evidenceBeforeSkip = byId("evidence").children;
    for (const hash of ["#investigation", "#unrelated", ""]) {
      location.hash = hash;
      events.get("hashchange")!();
      expect(byId("case-label").textContent).toStartWith("DEP-01");
      expect(byId("evidence").children).toBe(evidenceBeforeSkip);
    }
    location.hash = "#ROLLOUT-03";
    events.get("hashchange")!();
    expect(byId("case-title").textContent).toBe("Readiness probe returns 404");
    expect(chosen.attributes.get("aria-current")).toBe("false");
  } finally {
    for (const [name, descriptor] of saved) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else Reflect.deleteProperty(globalThis, name);
    }
  }
});
