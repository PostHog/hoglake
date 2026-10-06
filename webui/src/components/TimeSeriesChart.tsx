import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { ChartLine } from "../lib/metricsHistory";

// A thin uPlot wrapper (#290): one chart per metric family, lines from
// `familyChart`. uPlot is what Grafana draws time series with and it
// has no React layer of its own; the whole integration is this effect.
// The chart is created once per mount and fed with setData on every
// poll, so a 4 h window at 15 s (960 points × up to 12 lines) redraws
// in a frame rather than remounting.

/** Twelve hues that read on both themes; cycled past MAX_LINES never happens. */
const PALETTE = [
  "#6aa1ff",
  "#4cc38a",
  "#e5b567",
  "#ef7a85",
  "#b58cf6",
  "#4fc3d9",
  "#f2a44e",
  "#8fd14f",
  "#f06bb8",
  "#9aa5b5",
  "#d4c04a",
  "#62d7a8",
];

interface Props {
  lines: ChartLine[];
  /** Formats a y value for the axis and legend. */
  format: (v: number) => string;
  height?: number;
}

/** uPlot wants one x array and one aligned y array per series. */
export function alignLines(lines: ChartLine[]): uPlot.AlignedData {
  const stamps = [...new Set(lines.flatMap((l) => l.points.map((p) => p[0])))].sort(
    (a, b) => a - b,
  );
  const x = stamps.map((t) => t / 1000);
  const ys = lines.map((l) => {
    const at = new Map(l.points);
    return stamps.map((t) => at.get(t) ?? null);
  });
  return [x, ...ys];
}

function themeColor(name: string, fallback: string): string {
  try {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  } catch {
    return fallback;
  }
}

export function TimeSeriesChart({ lines, format, height = 160 }: Props) {
  const host = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  const formatRef = useRef(format);
  formatRef.current = format;
  const labels = lines.map((l) => l.label || "value").join("\u0000");

  // Axis and grid colors are read from the theme tokens when the chart
  // is built, so a theme or palette switch rebuilds it: watch the two
  // attributes lib/theme.ts stamps on <html>.
  const [themeKey, setThemeKey] = useState(0);
  useEffect(() => {
    if (typeof MutationObserver === "undefined") return;
    const mo = new MutationObserver(() => setThemeKey((k) => k + 1));
    mo.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "data-color-theme"],
    });
    return () => mo.disconnect();
  }, []);

  // (Re)create when the set of lines or the theme changes; feed data otherwise.
  useEffect(() => {
    const el = host.current;
    if (!el) return;
    plot.current?.destroy();
    const text = themeColor("--text-dim", "#8b919c");
    const grid = themeColor("--border", "#2e323a");
    const opts: uPlot.Options = {
      width: Math.max(200, el.clientWidth),
      height,
      cursor: { sync: { key: "metrics" } },
      legend: { live: true },
      scales: { x: { time: true } },
      axes: [
        { stroke: text, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid } },
        {
          stroke: text,
          grid: { stroke: grid, width: 1 },
          ticks: { stroke: grid },
          size: 92,
          values: (_u, splits) => splits.map((v) => formatRef.current(v)),
        },
      ],
      series: [
        {},
        ...lines.map((l, i) => ({
          label: l.label || "value",
          stroke: PALETTE[i % PALETTE.length],
          width: 1.5,
          spanGaps: false,
          value: (_u: uPlot, v: number | null) => (v === null ? "—" : formatRef.current(v)),
        })),
      ],
    };
    plot.current = new uPlot(opts, alignLines(lines), el);
    const ro =
      typeof ResizeObserver === "undefined"
        ? null
        : new ResizeObserver(() => {
            plot.current?.setSize({ width: Math.max(200, el.clientWidth), height });
          });
    ro?.observe(el);
    return () => {
      ro?.disconnect();
      plot.current?.destroy();
      plot.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [labels, height, themeKey]);

  useEffect(() => {
    plot.current?.setData(alignLines(lines));
  }, [lines]);

  return <div className="timeseries" ref={host} data-testid="timeseries" data-lines={lines.length} />;
}
