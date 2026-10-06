// In-browser time series over polled /metrics snapshots (#290).
//
// The page polls GET /metrics on an interval and hands every parsed
// snapshot to a MetricsHistory, which keeps one bounded ring buffer per
// series. History lives as long as the tab; nothing is stored anywhere
// else. Memory is the product of series × window: at the default 960
// points (4 h at 15 s) and 2,000 series that is 2,000 × 960 × 16 B of
// typed-array storage, about 30 MiB, with no per-point object overhead.
//
// Everything here is pure so `just webui test` covers it without a DOM:
// the key, the bound, the eviction, the counter-rate math and the
// histogram quantiles.

import type { MetricFamily } from "./prometheus";

/** [timestamp ms, value] */
export type Point = [number, number];

/** 4 h at the default 15 s poll. */
export const DEFAULT_MAX_POINTS = 960;

/** `name{a=1,b=2}` with labels in sorted order; the identity of a series. */
export function seriesKey(name: string, labels: Record<string, string>): string {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) return name;
  return `${name}{${keys.map((k) => `${k}=${labels[k]}`).join(",")}}`;
}

/** Labels as `a=1,b=2`, sorted, with some names excluded (the histogram `le`). */
export function labelsText(
  labels: Record<string, string>,
  exclude: readonly string[] = [],
): string {
  return Object.keys(labels)
    .filter((k) => !exclude.includes(k))
    .sort()
    .map((k) => `${k}=${labels[k]}`)
    .join(",");
}

class Ring {
  private readonly ts: Float64Array;
  private readonly vs: Float64Array;
  private start = 0;
  private count = 0;

  constructor(readonly capacity: number) {
    this.ts = new Float64Array(capacity);
    this.vs = new Float64Array(capacity);
  }

  push(t: number, v: number): void {
    const i = (this.start + this.count) % this.capacity;
    this.ts[i] = t;
    this.vs[i] = v;
    if (this.count < this.capacity) this.count++;
    else this.start = (this.start + 1) % this.capacity;
  }

  get length(): number {
    return this.count;
  }

  points(): Point[] {
    const out: Point[] = new Array(this.count);
    for (let k = 0; k < this.count; k++) {
      const i = (this.start + k) % this.capacity;
      out[k] = [this.ts[i], this.vs[i]];
    }
    return out;
  }
}

export interface SeriesInfo {
  key: string;
  /** Sample name, e.g. `hoglake_commit_lock_wait_seconds_bucket`. */
  name: string;
  labels: Record<string, string>;
  family: string;
}

interface Entry extends SeriesInfo {
  ring: Ring;
  lastTick: number;
}

export class MetricsHistory {
  private readonly entries = new Map<string, Entry>();
  private readonly byFamily = new Map<string, Set<string>>();
  private tick = 0;

  constructor(readonly maxPoints: number = DEFAULT_MAX_POINTS) {}

  /**
   * Record one snapshot at [ts]. Non-finite values (NaN, +Inf) are
   * skipped, so a series is only ever numbers. A series absent from
   * [maxPoints] consecutive snapshots is forgotten: its whole window has
   * scrolled past, and label churn (a table dropped, a consumer retired)
   * must not accumulate dead buffers for the life of the tab.
   */
  record(ts: number, families: MetricFamily[]): void {
    this.tick++;
    for (const family of families) {
      for (const sample of family.samples) {
        const v = Number(sample.value);
        if (!Number.isFinite(v)) continue;
        const key = seriesKey(sample.name, sample.labels);
        let entry = this.entries.get(key);
        if (!entry) {
          entry = {
            key,
            name: sample.name,
            labels: sample.labels,
            family: family.name,
            ring: new Ring(this.maxPoints),
            lastTick: this.tick,
          };
          this.entries.set(key, entry);
          let keys = this.byFamily.get(family.name);
          if (!keys) {
            keys = new Set();
            this.byFamily.set(family.name, keys);
          }
          keys.add(key);
        }
        entry.ring.push(ts, v);
        entry.lastTick = this.tick;
      }
    }
    for (const [key, entry] of this.entries) {
      if (this.tick - entry.lastTick >= this.maxPoints) {
        this.entries.delete(key);
        this.byFamily.get(entry.family)?.delete(key);
      }
    }
  }

  /** Snapshots recorded so far (the x-axis length of a never-absent series). */
  get ticks(): number {
    return this.tick;
  }

  get seriesCount(): number {
    return this.entries.size;
  }

  points(key: string): Point[] {
    return this.entries.get(key)?.ring.points() ?? [];
  }

  /** Every series recorded under [family], with their points. */
  family(family: string): (SeriesInfo & { points: Point[] })[] {
    const keys = this.byFamily.get(family);
    if (!keys) return [];
    const out: (SeriesInfo & { points: Point[] })[] = [];
    for (const key of keys) {
      const e = this.entries.get(key);
      if (e) {
        out.push({
          key: e.key,
          name: e.name,
          labels: e.labels,
          family: e.family,
          points: e.ring.points(),
        });
      }
    }
    return out;
  }
}

// -- derived series ------------------------------------------------------------

/**
 * Per-second rate between consecutive points of a counter. A drop is a
 * counter reset (the pod restarted), and the increase since the reset is
 * the current value, as Prometheus's `rate` treats it. The rate is
 * stamped at the later point; a series of n points yields n-1 rates.
 */
export function counterRates(points: Point[]): Point[] {
  const out: Point[] = [];
  for (let i = 1; i < points.length; i++) {
    const [t0, v0] = points[i - 1];
    const [t1, v1] = points[i];
    const dt = (t1 - t0) / 1000;
    if (dt <= 0) continue;
    const inc = v1 >= v0 ? v1 - v0 : v1;
    out.push([t1, inc / dt]);
  }
  return out;
}

export interface BucketSeries {
  /** Upper bound; `Infinity` for the +Inf bucket. */
  le: number;
  points: Point[];
}

/** The `le` label as a number; `Infinity` for +Inf. */
export function numericLe(le: string): number {
  return le === "+Inf" ? Number.POSITIVE_INFINITY : Number(le);
}

/**
 * Prometheus's histogram_quantile over the INCREASE of each bucket
 * between consecutive snapshots: the quantile of what was observed in
 * the last poll interval, stamped at the later snapshot. An interval
 * with no observations yields no point. Linear interpolation inside the
 * bucket; the +Inf bucket answers with the highest finite bound.
 */
export function histogramQuantiles(buckets: BucketSeries[], qs: number[]): Map<number, Point[]> {
  const sorted = [...buckets].sort((a, b) => a.le - b.le);
  const out = new Map<number, Point[]>(qs.map((q) => [q, []]));
  if (sorted.length === 0) return out;
  // Align on timestamps: a bucket that appeared later has no earlier points.
  const stamps = [...new Set(sorted.flatMap((b) => b.points.map((p) => p[0])))].sort((a, b) => a - b);
  const lookup = sorted.map((b) => new Map(b.points));
  for (let i = 1; i < stamps.length; i++) {
    const t0 = stamps[i - 1];
    const t1 = stamps[i];
    const deltas: number[] = [];
    let ok = true;
    for (let b = 0; b < sorted.length; b++) {
      const c0 = lookup[b].get(t0);
      const c1 = lookup[b].get(t1);
      if (c0 === undefined || c1 === undefined) {
        ok = false;
        break;
      }
      deltas.push(c1 >= c0 ? c1 - c0 : c1);
    }
    if (!ok) continue;
    // Cumulative buckets must be non-decreasing; a scrape mid-update can
    // violate that by a hair, so clamp.
    for (let b = 1; b < deltas.length; b++) {
      if (deltas[b] < deltas[b - 1]) deltas[b] = deltas[b - 1];
    }
    const total = deltas[deltas.length - 1];
    if (!(total > 0)) continue;
    for (const q of qs) {
      const rank = q * total;
      let b = 0;
      while (b < deltas.length - 1 && deltas[b] < rank) b++;
      let value: number;
      if (!Number.isFinite(sorted[b].le)) {
        value = b > 0 ? sorted[b - 1].le : 0;
      } else {
        const lower = b > 0 ? sorted[b - 1].le : 0;
        const countBelow = b > 0 ? deltas[b - 1] : 0;
        const inBucket = deltas[b] - countBelow;
        value =
          inBucket > 0
            ? lower + (sorted[b].le - lower) * ((rank - countBelow) / inBucket)
            : sorted[b].le;
      }
      out.get(q)!.push([t1, value]);
    }
  }
  return out;
}

// -- what a family's chart shows ---------------------------------------------

export interface ChartLine {
  label: string;
  points: Point[];
}

export interface FamilyChart {
  /** Sample name the y values are formatted by (`formatMetricValue`). */
  valueName: string;
  /** Appended to the y label: "/s" for a counter rate. */
  unitSuffix: string;
  lines: ChartLine[];
  /** Series not drawn, when the family has more than [MAX_LINES]. */
  omitted: number;
  /** Snapshots behind the longest line: a chart needs two to be a line. */
  samples: number;
}

/** Lines per chart; a family with hundreds of label sets shows its largest. */
export const MAX_LINES = 12;

/**
 * The chart for one family, from its recorded history. Counters chart
 * as per-second rates, gauges as values, histograms as p50 and p99 of
 * each interval's observations, quantile summaries as their quantiles.
 * Lines are the [MAX_LINES] with the largest latest value; the rest
 * are counted in `omitted`.
 */
export function familyChart(history: MetricsHistory, family: MetricFamily): FamilyChart | null {
  const recorded = history.family(family.name);
  if (recorded.length === 0) return null;
  let lines: ChartLine[] = [];
  let valueName = family.name;
  let unitSuffix = "";

  const buckets = recorded.filter((s) => s.name.endsWith("_bucket") && s.labels.le !== undefined);
  const quantiles = recorded.filter((s) => s.labels.quantile !== undefined);
  if (buckets.length > 0) {
    const groups = new Map<string, BucketSeries[]>();
    for (const b of buckets) {
      const key = labelsText(b.labels, ["le"]);
      (groups.get(key) ?? groups.set(key, []).get(key)!).push({
        le: numericLe(b.labels.le),
        points: b.points,
      });
    }
    for (const [key, group] of groups) {
      const q = histogramQuantiles(group, [0.5, 0.99]);
      const prefix = key === "" ? "" : `${key} `;
      lines.push({ label: `${prefix}p50`, points: q.get(0.5)! });
      lines.push({ label: `${prefix}p99`, points: q.get(0.99)! });
    }
  } else if (quantiles.length > 0) {
    lines = quantiles.map((s) => ({ label: labelsText(s.labels), points: s.points }));
  } else if (family.type === "counter") {
    unitSuffix = "/s";
    lines = recorded
      .filter((s) => !s.name.endsWith("_created"))
      .map((s) => ({ label: labelsText(s.labels), points: counterRates(s.points) }));
    valueName = recorded[0].name;
  } else if (family.type === "summary") {
    // Quantile-less summary: the observation rate from _count.
    unitSuffix = "/s";
    lines = recorded
      .filter((s) => s.name.endsWith("_count"))
      .map((s) => ({ label: labelsText(s.labels), points: counterRates(s.points) }));
    valueName = `${family.name}_count`;
  } else {
    lines = recorded.map((s) => ({ label: labelsText(s.labels), points: s.points }));
    valueName = recorded[0].name;
  }

  lines = lines.filter((l) => l.points.length > 0);
  if (lines.length === 0) return null;
  const last = (l: ChartLine) => l.points[l.points.length - 1][1];
  lines.sort((a, b) => last(b) - last(a));
  const omitted = Math.max(0, lines.length - MAX_LINES);
  const samples = Math.max(...recorded.map((s) => s.points.length));
  return { valueName, unitSuffix, lines: lines.slice(0, MAX_LINES), omitted, samples };
}
