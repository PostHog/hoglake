import type { SortState } from "../lib/sort";

/**
 * A column header that sorts its table.
 *
 * A real <button> inside the <th> rather than a click handler on the th:
 * keyboard reachable and announced as activatable, which a clickable
 * cell is not. `aria-sort` carries the current direction, so the arrow
 * is not the only place the state exists.
 */
export function SortableTh<K extends string>({
  label,
  sortKey,
  sort,
  onSort,
  numeric,
  tooltip,
}: {
  label: string;
  sortKey: K;
  sort: SortState<K> | null;
  onSort: (key: K) => void;
  numeric?: boolean;
  tooltip?: string;
}) {
  const active = sort?.key === sortKey;
  const arrow = !active ? "" : sort.desc ? " ↓" : " ↑";
  return (
    <th
      className={numeric ? "num sortable" : "sortable"}
      aria-sort={active ? (sort.desc ? "descending" : "ascending") : "none"}
    >
      <button
        type="button"
        className="th-sort"
        onClick={() => onSort(sortKey)}
        title={tooltip}
      >
        {tooltip ? <span className="th-hint">{label}</span> : label}
        {arrow}
      </button>
    </th>
  );
}
