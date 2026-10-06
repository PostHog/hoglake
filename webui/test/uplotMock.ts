// uPlot under jsdom: the real module reads matchMedia at import time and
// draws on a canvas, neither of which jsdom has. The page tests care
// about WHAT the chart was given, not how it was painted, so this fake
// records every instance's options and data.

export interface FakePlot {
  opts: { series: { label?: string }[] };
  data: unknown[][];
  destroyed: boolean;
}

export const plots: FakePlot[] = [];

export class FakeUPlot implements FakePlot {
  data: unknown[][];
  destroyed = false;
  constructor(
    public opts: { series: { label?: string }[] },
    data: unknown[][],
    _host: HTMLElement,
  ) {
    this.data = data;
    plots.push(this);
  }
  setData(data: unknown[][]) {
    this.data = data;
  }
  setSize() {}
  destroy() {
    this.destroyed = true;
  }
}
