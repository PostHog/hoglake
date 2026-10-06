import { describe, expect, it } from "vitest";
import {
  counterRates,
  familyChart,
  histogramQuantiles,
  labelsText,
  MAX_LINES,
  MetricsHistory,
  seriesKey,
  type Point,
} from "../src/lib/metricsHistory";
import { parsePrometheusText } from "../src/lib/prometheus";

const gauge = (v: number) => parsePrometheusText(`# TYPE g gauge\ng ${v}\n`);

describe("seriesKey", () => {
  it("is the name plus sorted labels, and bare for an unlabeled sample", () => {
    expect(seriesKey("x", {})).toBe("x");
    expect(seriesKey("x", { b: "2", a: "1" })).toBe("x{a=1,b=2}");
    expect(labelsText({ le: "1", route: "/v1" }, ["le"])).toBe("route=/v1");
  });
});

describe("MetricsHistory", () => {
  it("keeps the newest maxPoints per series and drops the oldest", () => {
    const h = new MetricsHistory(5);
    for (let i = 1; i <= 8; i++) h.record(i * 1000, gauge(i));
    expect(h.points("g")).toEqual([
      [4000, 4],
      [5000, 5],
      [6000, 6],
      [7000, 7],
      [8000, 8],
    ]);
    expect(h.ticks).toBe(8);
  });

  it("skips non-finite values rather than recording them", () => {
    const h = new MetricsHistory(10);
    h.record(1000, parsePrometheusText("# TYPE g gauge\ng NaN\n"));
    h.record(2000, parsePrometheusText("# TYPE g gauge\ng +Inf\n"));
    h.record(3000, gauge(3));
    expect(h.points("g")).toEqual([[3000, 3]]);
  });

  it("forgets a series absent for a whole window, keeps one that returns", () => {
    const h = new MetricsHistory(3);
    const two = parsePrometheusText('# TYPE g gauge\ng{t="a"} 1\ng{t="b"} 2\n');
    const one = parsePrometheusText('# TYPE g gauge\ng{t="a"} 1\n');
    h.record(1000, two);
    expect(h.seriesCount).toBe(2);
    h.record(2000, one);
    h.record(3000, one);
    expect(h.seriesCount).toBe(2); // absent 2 of 3: still remembered
    h.record(4000, one);
    expect(h.seriesCount).toBe(1); // absent 3 of 3: gone
    expect(h.family("g").map((s) => s.key)).toEqual(["g{t=a}"]);
  });

  it("groups samples under their declared family, including histogram components", () => {
    const h = new MetricsHistory(10);
    h.record(
      1000,
      parsePrometheusText(
        [
          "# TYPE h histogram",
          'h_bucket{le="1"} 1',
          'h_bucket{le="+Inf"} 2',
          "h_count 2",
          "h_sum 1.5",
        ].join("\n"),
      ),
    );
    expect(h.family("h").map((s) => s.name).sort()).toEqual([
      "h_bucket",
      "h_bucket",
      "h_count",
      "h_sum",
    ]);
  });
});

describe("counterRates", () => {
  it("is the increase per second between consecutive points", () => {
    const pts: Point[] = [
      [0, 100],
      [15000, 130],
      [45000, 190],
    ];
    expect(counterRates(pts)).toEqual([
      [15000, 2],
      [45000, 2],
    ]);
  });

  it("treats a drop as a reset and counts from zero", () => {
    const pts: Point[] = [
      [0, 100],
      [10000, 5],
    ];
    expect(counterRates(pts)).toEqual([[10000, 0.5]]);
  });

  it("yields nothing for fewer than two points or a zero interval", () => {
    expect(counterRates([[0, 1]])).toEqual([]);
    expect(
      counterRates([
        [0, 1],
        [0, 2],
      ]),
    ).toEqual([]);
  });
});

describe("histogramQuantiles", () => {
  // Buckets le=1,2,4,+Inf. Between t=0 and t=10s the increases are
  // 10, 20, 30, 40 cumulative: 10 obs ≤1, 10 in (1,2], 10 in (2,4], 10 above.
  const buckets = [
    { le: 1, points: [[0, 0], [10000, 10]] as Point[] },
    { le: 2, points: [[0, 0], [10000, 20]] as Point[] },
    { le: 4, points: [[0, 0], [10000, 30]] as Point[] },
    { le: Infinity, points: [[0, 0], [10000, 40]] as Point[] },
  ];

  it("interpolates inside the bucket holding the rank", () => {
    const q = histogramQuantiles(buckets, [0.5, 0.25, 0.75]);
    // rank 20 lands exactly at the top of (1,2]
    expect(q.get(0.5)).toEqual([[10000, 2]]);
    // rank 10: top of the first bucket (0,1]
    expect(q.get(0.25)).toEqual([[10000, 1]]);
    // rank 30: top of (2,4]
    expect(q.get(0.75)).toEqual([[10000, 4]]);
  });

  it("answers the +Inf bucket with the highest finite bound", () => {
    expect(histogramQuantiles(buckets, [0.99])!.get(0.99)).toEqual([[10000, 4]]);
  });

  it("emits no point for an interval with no observations", () => {
    const flat = buckets.map((b) => ({
      le: b.le,
      points: [...b.points, [20000, b.points[1][1]]] as Point[],
    }));
    expect(histogramQuantiles(flat, [0.5]).get(0.5)).toEqual([[10000, 2]]);
  });

  it("is reset-safe", () => {
    const reset = buckets.map((b) => ({
      le: b.le,
      points: [[0, 1000], [10000, b.points[1][1]]] as Point[],
    }));
    expect(histogramQuantiles(reset, [0.5]).get(0.5)).toEqual([[10000, 2]]);
  });
});

describe("familyChart", () => {
  const text = (lines: string[]) => parsePrometheusText(lines.join("\n"));

  it("charts a counter as rates, a gauge as values", () => {
    const h = new MetricsHistory(10);
    const snap = (c: number, g: number) =>
      text(["# TYPE c counter", `c_total ${c}`, "# TYPE g gauge", `g ${g}`]);
    h.record(0, snap(0, 5));
    h.record(10000, snap(50, 7));
    const [cFam, gFam] = snap(50, 7);
    const c = familyChart(h, cFam)!;
    expect(c.unitSuffix).toBe("/s");
    expect(c.lines).toEqual([{ label: "", points: [[10000, 5]] }]);
    const g = familyChart(h, gFam)!;
    expect(g.unitSuffix).toBe("");
    expect(g.lines[0].points).toEqual([
      [0, 5],
      [10000, 7],
    ]);
  });

  it("charts a histogram as p50 and p99 per label set", () => {
    const h = new MetricsHistory(10);
    const snap = (n: number) =>
      text([
        "# TYPE h histogram",
        `h_bucket{route="a",le="1"} ${n}`,
        `h_bucket{route="a",le="+Inf"} ${2 * n}`,
        `h_count{route="a"} ${2 * n}`,
      ]);
    h.record(0, snap(0));
    h.record(10000, snap(10));
    const chart = familyChart(h, snap(10)[0])!;
    expect(chart.lines.map((l) => l.label).sort()).toEqual(["route=a p50", "route=a p99"]);
    expect(chart.lines.find((l) => l.label.endsWith("p50"))!.points).toEqual([[10000, 1]]);
  });

  it("draws the largest MAX_LINES series and counts the rest", () => {
    const h = new MetricsHistory(10);
    const lines = ["# TYPE g gauge"];
    for (let i = 0; i < MAX_LINES + 5; i++) lines.push(`g{i="${i}"} ${i}`);
    const fams = text(lines);
    h.record(0, fams);
    const chart = familyChart(h, fams[0])!;
    expect(chart.lines).toHaveLength(MAX_LINES);
    expect(chart.lines[0].label).toBe(`i=${MAX_LINES + 4}`);
    expect(chart.omitted).toBe(5);
  });

  it("is null for a family with nothing recorded", () => {
    const h = new MetricsHistory(10);
    expect(familyChart(h, { name: "nothing", samples: [] })).toBeNull();
  });
});
