import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchMetricsText } from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import { TimeSeriesChart } from "../components/TimeSeriesChart";
import { formatTime } from "../lib/format";
import {
  DEFAULT_MAX_POINTS,
  familyChart,
  MetricsHistory,
  type FamilyChart,
} from "../lib/metricsHistory";
import { readStringList, useStoredPref, writeStringList } from "../lib/prefs";
import {
  formatMetricValue,
  parsePrometheusText,
  type MetricFamily,
  type MetricSample,
} from "../lib/prometheus";

// The snapshot view, polled (#290): the page refetches /metrics on an
// interval, hands each snapshot to a MetricsHistory that lives as long
// as the tab, and draws every family's history above its snapshot
// cards. Nothing changes server-side; the Refresh button still works.
// Families and series are still compared WITHIN a snapshot for the
// bars and tiles; the charts are where time comes in.

/** Poll interval choices, seconds; the window is DEFAULT_MAX_POINTS polls. */
const POLL_SECONDS = ["5", "15", "30", "60"] as const;
const POLL_KEY = "hoglake-metrics-poll";
const PINS_KEY = "hoglake-metrics-pins";

function windowText(pollSeconds: number): string {
  const minutes = (DEFAULT_MAX_POINTS * pollSeconds) / 60;
  return minutes >= 60 ? `${(minutes / 60).toFixed(minutes % 60 === 0 ? 0 : 1)} h` : `${minutes} min`;
}

/** The pin button in a family card's header. */
function PinButton({
  name,
  pinned,
  onToggle,
}: {
  name: string;
  pinned: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      className={`pin-toggle${pinned ? " pinned" : ""}`}
      onClick={onToggle}
      title={pinned ? "Unpin: back to its place in the list" : "Pin: keep this chart at the top"}
      aria-label={`${pinned ? "unpin" : "pin"} ${name}`}
      aria-pressed={pinned}
    >
      {pinned ? "★" : "☆"}
    </button>
  );
}

/** The maximize button in a family card's header: the card alone, full viewport. */
function ExpandButton({ name, onExpand }: { name: string; onExpand: () => void }) {
  return (
    <button
      type="button"
      className="pin-toggle"
      onClick={onExpand}
      title="Expand: this family alone, full size (Esc closes)"
      aria-label={`expand ${name}`}
    >
      ⤢
    </button>
  );
}

/**
 * The chart for a family, when its history holds something to draw. A
 * family that has been sampled but yields no line (a histogram with no
 * observations in the window, so no quantile has a value) says so,
 * rather than looking like a family nothing has been recorded for.
 */
function FamilyHistory({
  chart,
  height,
  ticks,
}: {
  chart: FamilyChart | null;
  height?: number;
  ticks: number;
}) {
  if (ticks < 2) return null;
  if (!chart) {
    return <p className="subtle metric-history-note">no observations in the window yet</p>;
  }
  const format = (v: number) =>
    `${formatMetricValue(chart.valueName, String(v))}${chart.unitSuffix}`;
  return (
    <div className="metric-history">
      <TimeSeriesChart lines={chart.lines} format={format} height={height} />
      {chart.omitted > 0 && (
        <p className="subtle metric-history-note">
          showing the {chart.lines.length} largest of {chart.lines.length + chart.omitted} series
        </p>
      )}
    </div>
  );
}

/** Stable key for a sample's label set, optionally ignoring some label names. */
function labelsKey(
  labels: Record<string, string>,
  exclude: readonly string[] = [],
): string {
  return Object.entries(labels)
    .filter(([k]) => !exclude.includes(k))
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

function LabelChips({ text }: { text: string }) {
  if (text === "") return null;
  return (
    <span className="hbar-labels">
      {text.split(",").map((kv) => (
        <span key={kv} className="badge label-chip">
          {kv}
        </span>
      ))}
    </span>
  );
}

/**
 * The snapshot's per-series detail, closed by default (#290): the chart
 * above it is the view, and on prod-us the per-table gauges carry
 * hundreds of label sets per family, so a card that lists them is a
 * page that scrolls forever. One click opens the list in place.
 */
function SeriesDisclosure({ count, children }: { count: number; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="series-disclosure">
      <button
        type="button"
        className="hbars-more"
        onClick={() => setOpen((x) => !x)}
        aria-expanded={open}
      >
        {open ? "hide" : "show"} {count} series
      </button>
      {open && children}
    </div>
  );
}

/** Horizontal bars: one per label set, largest first, scaled to the family max. */
function HorizontalBars({ samples }: { samples: MetricSample[] }) {
  const ranked = samples
    .map((s) => ({ s, v: Number(s.value) }))
    .sort((a, b) => (Number.isFinite(b.v) ? b.v : -Infinity) - (Number.isFinite(a.v) ? a.v : -Infinity));
  const max = Math.max(0, ...ranked.map((r) => r.v).filter((v) => Number.isFinite(v)));
  return (
    <div className="hbars">
      {ranked.map(({ s, v }, i) => {
        const pct =
          max > 0 && Number.isFinite(v) ? Math.max(0, (v / max) * 100) : 0;
        return (
          <div className="hbar-row" key={i}>
            <LabelChips text={labelsKey(s.labels)} />
            <div className="hbar-track">
              <div className="hbar-fill" style={{ width: `${pct}%` }} />
            </div>
            <span className="mono hbar-value" title={s.value}>
              {formatMetricValue(s.name, s.value)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/** Scalar family: one unlabeled sample → a stat tile, with its history beside. */
function StatTile({
  family,
  sample,
  chart,
  pinned,
  onTogglePin,
  onExpand,
  chartHeight,
  ticks,
}: {
  family: MetricFamily;
  sample: MetricSample;
  chart: FamilyChart | null;
  pinned: boolean;
  onTogglePin: () => void;
  onExpand?: () => void;
  chartHeight?: number;
  ticks: number;
}) {
  return (
    <section className={`metric-family stat-tile${onExpand ? "" : " expanded"}`}>
      <div className="stat-tile-head">
        <div className="stat-tile-value mono" title={sample.value}>
          {formatMetricValue(sample.name, sample.value)}
        </div>
        <span className="card-actions">
          <PinButton name={family.name} pinned={pinned} onToggle={onTogglePin} />
          {onExpand && <ExpandButton name={family.name} onExpand={onExpand} />}
        </span>
      </div>
      <div className="mono metric-family-name">
        {family.name} <span className="badge">{family.type ?? "untyped"}</span>
      </div>
      {family.help && <p className="metric-help">{family.help}</p>}
      <FamilyHistory chart={chart} height={chartHeight} ticks={ticks} />
    </section>
  );
}

const numericLe = (le: string): number =>
  le === "+Inf" ? Number.POSITIVE_INFINITY : Number(le);

/** Cumulative `le` buckets → per-bucket deltas, rendered as a bar strip. */
function BucketStrip({ buckets }: { buckets: MetricSample[] }) {
  const sorted = [...buckets].sort(
    (a, b) => numericLe(a.labels.le ?? "") - numericLe(b.labels.le ?? ""),
  );
  let prev = 0;
  const deltas = sorted.map((b) => {
    const cum = Number(b.value);
    const d = Number.isFinite(cum) ? Math.max(0, cum - prev) : 0;
    if (Number.isFinite(cum)) prev = cum;
    return { le: b.labels.le ?? "?", count: d };
  });
  const max = Math.max(1, ...deltas.map((d) => d.count));
  return (
    <div className="bucket-strip" data-testid="bucket-strip">
      {deltas.map((d, i) => (
        <div
          key={i}
          className="bucket-bar"
          title={`≤ ${d.le}: ${d.count.toLocaleString("en-US")}`}
          style={{ height: `${d.count > 0 ? Math.max(4, (d.count / max) * 100) : 1}%` }}
        />
      ))}
    </div>
  );
}

/** One histogram series: bucket strip plus count and mean. */
function HistogramSeries({
  family,
  seriesLabels,
  buckets,
  count,
  sum,
}: {
  family: MetricFamily;
  seriesLabels: string;
  buckets: MetricSample[];
  count?: MetricSample;
  sum?: MetricSample;
}) {
  const n = count ? Number(count.value) : NaN;
  const total = sum ? Number(sum.value) : NaN;
  const mean =
    Number.isFinite(n) && n > 0 && Number.isFinite(total) ? total / n : null;
  return (
    <div className="histogram-series">
      <LabelChips text={seriesLabels} />
      <BucketStrip buckets={buckets} />
      <div className="histogram-stats">
        <span>
          count{" "}
          <span className="mono" title={count?.value}>
            {count ? formatMetricValue(`${family.name}_count`, count.value) : "—"}
          </span>
        </span>
        <span>
          mean{" "}
          <span className="mono">
            {mean === null
              ? "—"
              : formatMetricValue(family.name, String(mean))}
          </span>
        </span>
      </div>
    </div>
  );
}

/** Quantile-labeled summary series: quantiles as labeled bars. */
function QuantileSeries({
  familyName,
  seriesLabels,
  quantiles,
}: {
  familyName: string;
  seriesLabels: string;
  quantiles: MetricSample[];
}) {
  const bars = [...quantiles].sort(
    (a, b) => Number(a.labels.quantile) - Number(b.labels.quantile),
  );
  const max = Math.max(
    0,
    ...bars.map((q) => Number(q.value)).filter(Number.isFinite),
  );
  return (
    <div className="histogram-series">
      <LabelChips text={seriesLabels} />
      <div className="hbars">
        {bars.map((q, i) => {
          const v = Number(q.value);
          const pct =
            max > 0 && Number.isFinite(v) ? Math.max(0, (v / max) * 100) : 0;
          return (
            <div className="hbar-row" key={i}>
              <span className="badge label-chip">q={q.labels.quantile}</span>
              <div className="hbar-track">
                <div className="hbar-fill" style={{ width: `${pct}%` }} />
              </div>
              <span className="mono hbar-value" title={q.value}>
                {formatMetricValue(familyName, q.value)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function FamilyCard({
  family,
  chart,
  pinned,
  onTogglePin,
  onExpand,
  chartHeight,
  ticks,
}: {
  family: MetricFamily;
  chart: FamilyChart | null;
  pinned: boolean;
  onTogglePin: () => void;
  /** Absent inside the expanded view, where the card already is full size. */
  onExpand?: () => void;
  chartHeight?: number;
  /** Snapshots recorded in this tab; below two there is nothing to draw anywhere. */
  ticks: number;
}) {
  const buckets = family.samples.filter(
    (s) => s.name.endsWith("_bucket") && s.labels.le !== undefined,
  );
  const quantiles = family.samples.filter(
    (s) => s.labels.quantile !== undefined,
  );
  // _count/_sum are meter components only for histogram/summary families and
  // only when they EXTEND the family name — a gauge named *_count (e.g.
  // hoglake_table_count) is its own scalar sample, not a component.
  const isMeter = family.type === "histogram" || family.type === "summary";
  const counts = family.samples.filter(
    (s) => isMeter && s.name !== family.name && s.name.endsWith("_count"),
  );
  const sums = family.samples.filter(
    (s) => isMeter && s.name !== family.name && s.name.endsWith("_sum"),
  );
  const plain = family.samples.filter(
    (s) =>
      !buckets.includes(s) &&
      !quantiles.includes(s) &&
      !counts.includes(s) &&
      !sums.includes(s),
  );

  let body: ReactNode;
  if (buckets.length > 0) {
    // Histogram: one strip per label set (le excluded).
    const groups = new Map<string, MetricSample[]>();
    for (const b of buckets) {
      const key = labelsKey(b.labels, ["le"]);
      (groups.get(key) ?? groups.set(key, []).get(key)!).push(b);
    }
    body = (
      <SeriesDisclosure count={groups.size}>
        {[...groups.entries()].map(([key, group]) => (
          <HistogramSeries
            key={key}
            family={family}
            seriesLabels={key}
            buckets={group}
            count={counts.find((c) => labelsKey(c.labels) === key)}
            sum={sums.find((c) => labelsKey(c.labels) === key)}
          />
        ))}
      </SeriesDisclosure>
    );
  } else if (quantiles.length > 0) {
    // Summary with quantile samples: labeled bars per series.
    const groups = new Map<string, MetricSample[]>();
    for (const q of quantiles) {
      const key = labelsKey(q.labels, ["quantile"]);
      (groups.get(key) ?? groups.set(key, []).get(key)!).push(q);
    }
    body = (
      <SeriesDisclosure count={groups.size}>
        {[...groups.entries()].map(([key, group]) => (
          <QuantileSeries
            key={key}
            familyName={family.name}
            seriesLabels={key}
            quantiles={group}
          />
        ))}
      </SeriesDisclosure>
    );
  } else if (family.type === "summary" && counts.length > 0) {
    // Quantile-less summary (count/sum[/max]): count and mean per series.
    const rows = counts.map((c) => {
      const key = labelsKey(c.labels);
      const sum = sums.find((s) => labelsKey(s.labels) === key);
      const n = Number(c.value);
      const mean =
        sum && Number.isFinite(n) && n > 0 ? Number(sum.value) / n : null;
      return (
        <div className="histogram-stats" key={key}>
          <LabelChips text={key} />
          <span>
            count{" "}
            <span className="mono" title={c.value}>
              {formatMetricValue(c.name, c.value)}
            </span>
          </span>
          <span>
            mean{" "}
            <span className="mono">
              {mean === null ? "—" : formatMetricValue(family.name, String(mean))}
            </span>
          </span>
        </div>
      );
    });
    body = <SeriesDisclosure count={rows.length}>{rows}</SeriesDisclosure>;
  } else if (plain.length === 1 && Object.keys(plain[0].labels).length === 0) {
    // Scalar → stat tile (its own card layout).
    return (
      <StatTile
        family={family}
        sample={plain[0]}
        chart={chart}
        pinned={pinned}
        onTogglePin={onTogglePin}
        onExpand={onExpand}
        chartHeight={chartHeight}
        ticks={ticks}
      />
    );
  } else if (plain.length > 0) {
    body = (
      <SeriesDisclosure count={plain.length}>
        <HorizontalBars samples={plain} />
      </SeriesDisclosure>
    );
  } else {
    body = <p className="empty">No samples.</p>;
  }

  return (
    <section className="metric-family">
      <header className="metric-family-head">
        <span className="mono metric-family-name">{family.name}</span>
        <span className="badge">{family.type ?? "untyped"}</span>
        <span className="card-actions">
          <PinButton name={family.name} pinned={pinned} onToggle={onTogglePin} />
          {onExpand && <ExpandButton name={family.name} onExpand={onExpand} />}
        </span>
      </header>
      {family.help && <p className="metric-help">{family.help}</p>}
      <FamilyHistory chart={chart} height={chartHeight} ticks={ticks} />
      {body}
    </section>
  );
}

export function MetricsPage() {
  const [filter, setFilter] = useState("");
  const [pollSeconds, setPollSeconds] = useStoredPref(POLL_KEY, POLL_SECONDS, "15");
  const query = useQuery({
    queryKey: ["metrics"],
    queryFn: fetchMetricsText,
    refetchOnWindowFocus: false,
    // The poll. Background tabs stop polling (the default), so history
    // has gaps where the tab was hidden rather than a stale wall.
    refetchInterval: Number(pollSeconds) * 1000,
  });

  const families = useMemo(
    () => (query.data === undefined ? [] : parsePrometheusText(query.data)),
    [query.data],
  );

  // History lives in a ref for the life of the page; `recorded` bumps
  // so the charts re-derive after each snapshot lands. Keyed on
  // dataUpdatedAt, not data: an identical snapshot is still a point.
  const history = useRef(new MetricsHistory());
  const [recorded, setRecorded] = useState(0);
  useEffect(() => {
    if (query.dataUpdatedAt > 0 && families.length > 0) {
      history.current.record(query.dataUpdatedAt, families);
      setRecorded((n) => n + 1);
    }
  }, [query.dataUpdatedAt, families]);

  const [pins, setPins] = useState<string[]>(() => readStringList(PINS_KEY));

  // One family, full viewport, until Esc or the close button. The chart
  // inside is the same uPlot fed by the same history; only its height
  // and the container width differ.
  const [expanded, setExpanded] = useState<string | null>(null);
  useEffect(() => {
    if (expanded === null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setExpanded(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [expanded]);
  const togglePin = (name: string) => {
    const next = pins.includes(name) ? pins.filter((p) => p !== name) : [...pins, name];
    writeStringList(PINS_KEY, next);
    setPins(next);
  };

  // Charts are derived per render from the history; `recorded` is the
  // dependency that makes that happen after each poll.
  const charts = useMemo(() => {
    const out = new Map<string, FamilyChart | null>();
    for (const f of families) out.set(f.name, familyChart(history.current, f));
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [families, recorded]);

  const needle = filter.trim().toLowerCase();
  const matches = (f: MetricFamily) =>
    needle === "" ||
    f.name.toLowerCase().includes(needle) ||
    (f.help ?? "").toLowerCase().includes(needle);

  const byName = new Map(families.map((f) => [f.name, f]));
  // Pinned families first, in pin order, whatever the filter says: a pin
  // is a deliberate "keep this in view".
  const pinned = pins.map((n) => byName.get(n)).filter((f): f is MetricFamily => f !== undefined);
  const hoglake = families.filter(
    (f) => f.name.startsWith("hoglake_") && !pins.includes(f.name) && matches(f),
  );
  const other = families.filter(
    (f) => !f.name.startsWith("hoglake_") && !pins.includes(f.name) && matches(f),
  );
  const card = (f: MetricFamily) => (
    <FamilyCard
      key={f.name}
      family={f}
      chart={charts.get(f.name) ?? null}
      pinned={pins.includes(f.name)}
      onTogglePin={() => togglePin(f.name)}
      onExpand={() => setExpanded(f.name)}
      ticks={history.current.ticks}
    />
  );
  const expandedFamily = expanded === null ? undefined : byName.get(expanded);

  return (
    <section>
      {expandedFamily && (
        <div className="metric-focus" role="dialog" aria-modal="true" aria-label={expandedFamily.name}>
          <div className="metric-focus-bar">
            <span className="subtle">Esc closes</span>
            <button
              type="button"
              className="pin-toggle"
              onClick={() => setExpanded(null)}
              aria-label={`close ${expandedFamily.name}`}
              title="Close (Esc)"
            >
              ✕
            </button>
          </div>
          <FamilyCard
            family={expandedFamily}
            chart={charts.get(expandedFamily.name) ?? null}
            pinned={pins.includes(expandedFamily.name)}
            onTogglePin={() => togglePin(expandedFamily.name)}
            chartHeight={Math.max(240, Math.floor(window.innerHeight * 0.55))}
            ticks={history.current.ticks}
          />
        </div>
      )}
      <h2>
        Metrics <span className="subtle">/metrics, polled</span>
      </h2>
      <div className="form-row metrics-toolbar">
        <label>
          filter
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="hoglake_commits"
            aria-label="filter metrics"
          />
        </label>
        <label>
          poll every
          <select
            value={pollSeconds}
            onChange={(e) => setPollSeconds(e.target.value as (typeof POLL_SECONDS)[number])}
            aria-label="poll interval"
          >
            {POLL_SECONDS.map((s) => (
              <option key={s} value={s}>
                {s} s
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
        >
          {query.isFetching ? "Refreshing…" : "Refresh"}
        </button>
        {query.dataUpdatedAt > 0 && (
          <span className="subtle metrics-fetched-at">
            fetched {formatTime(new Date(query.dataUpdatedAt).toISOString())}
            {" · "}
            {history.current.ticks} {history.current.ticks === 1 ? "sample" : "samples"} in this
            tab, {windowText(Number(pollSeconds))} window
            {history.current.ticks < 2 && " · charts appear with the next sample"}
          </span>
        )}
      </div>
      {query.isError && <ErrorBox error={query.error} />}
      {query.isPending && <SkeletonBlock />}
      {query.data !== undefined && (
        <>
          {pinned.length === 0 && hoglake.length === 0 && other.length === 0 && (
            <p className="empty">
              {needle === ""
                ? "The server exposed no metric families."
                : "No metric families match the filter."}
            </p>
          )}
          {pinned.length > 0 && (
            <div className="metrics-pinned" data-testid="metrics-pinned">
              {pinned.map(card)}
            </div>
          )}
          {hoglake.map(card)}
          {other.length > 0 && (
            <details className="metric-rest">
              <summary>
                everything else ({other.length}{" "}
                {other.length === 1 ? "family" : "families"})
              </summary>
              {other.map(card)}
            </details>
          )}
        </>
      )}
    </section>
  );
}
