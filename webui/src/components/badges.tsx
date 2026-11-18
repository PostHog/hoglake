import type { SnapshotChange, StatsState } from "../api/types";

export function StatsStateBadge({ state }: { state: StatsState }) {
  return <span className={`badge stats-${state}`}>{state}</span>;
}

export function ChangeBadge({ change }: { change: SnapshotChange }) {
  return (
    <span className="badge change-badge">
      {change.kind}
      {change.object_id !== undefined && (
        <span className="change-object">#{change.object_id}</span>
      )}
    </span>
  );
}

/** A remote schema or table: read-only, served by another Hoglake. */
export function RemoteBadge() {
  return (
    <span className="badge remote" title="Read-only: served by another Hoglake">
      remote
    </span>
  );
}

/** The one line a remote schema's pages carry under their heading. */
export function RemoteLine() {
  return <p className="subtle">Read-only: this schema is served by another Hoglake.</p>;
}
