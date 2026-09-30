import { useEffect, useId, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { listSnapshots } from "../api/client";
import { addInt64, compareInt64 } from "../api/int64";
import type { Int64 } from "../api/types";
import { formatRelativeAge, formatTime } from "../lib/format";

/**
 * What one snapshot id resolves to — THREE outcomes, because they are
 * three different facts and only one of them is about expiry:
 *
 * - `found`: the snapshot is retained and this is its commit time.
 * - `expired`: the catalog's floor has moved PAST the id. The listing
 *   answered with a later snapshot, which is the proof.
 * - `absent`: there is nothing at or above the id — it is above head, or
 *   the catalog has no snapshots at all. Saying "expired" here would be a
 *   fabrication: the id was never committed, not aged out.
 */
type Lookup =
  | { state: "found"; time: string }
  | { state: "expired" }
  | { state: "absent" };

/**
 * Snapshot id 0 is the NEVER-COMMITTED sentinel, not an id.
 * `V1__init.sql` declares `last_snapshot_id` and `earliest_snapshot_id`
 * `DEFAULT 0`, and a commit allocates `last_snapshot_id + 1`, so the
 * first real snapshot is 1 and no row ever carries 0. A fresh catalog
 * therefore shows head 0, and a catalog whose expiry has never run shows
 * floor 0 — the two most common ids in the console. Probing for them
 * would return snapshot 1 and "prove" an expiry that never happened.
 */
const NEVER_COMMITTED = "0";

/**
 * Continuous hover before the tooltip opens and the lookup is armed.
 *
 * It gates BOTH, together. Without it, dragging the pointer down a
 * listing's snapshot column fires one request per row crossed (the cache
 * dedupes repeats of ONE id and can do nothing about fifty different
 * ones) and leaves a stack of open boxes behind the cursor, each reading
 * "looking up snapshot…" about a request that does not exist. Opening on
 * intent rather than on entry removes both.
 */
const DWELL_MS = 150;

/**
 * Grace period before a tooltip closes on mouseleave, so the pointer can
 * cross the gap into the tooltip and read (or select) it — WCAG 1.4.13
 * Hoverable — without it flickering shut on the way.
 *
 * NOT covered by the suite, and it cannot be: the gap it exists for is
 * the 4px `margin-bottom` between the trigger's box and the tooltip's,
 * which is real geometry. jsdom does no layout and no hit testing, so
 * nothing there ever crosses it — a test that drove the DOM would be
 * exercising element containment, not the delay. Setting this to 0 breaks
 * the browser and no test; changing it needs a real browser to check.
 */
const CLOSE_DELAY_MS = 100;

/** Cache key for one (catalog, id) pair. */
function snapshotTimeKey(catalog: string, id: Int64): unknown[] {
  return ["snapshot-time", catalog, id];
}

/**
 * Date one snapshot id with the snapshot listing — no dedicated
 * get-one-snapshot endpoint exists, and this needs no server change.
 *
 * `after` is an EXCLUSIVE lower bound, so `id - 1` makes `id` the first
 * row a page can hold, and `limit=1` makes it the only one. What comes
 * back distinguishes all three outcomes: `id` itself, a LATER id (the
 * floor has passed it), or nothing at all (above head). Never called for
 * the 0 sentinel — the component short-circuits it — so the subtraction
 * cannot go negative.
 */
async function lookupSnapshotTime(
  catalog: string,
  id: Int64,
): Promise<Lookup> {
  const page = await listSnapshots(catalog, { after: addInt64(id, -1), limit: 1 });
  const first = page.snapshots[0];
  if (first === undefined) return { state: "absent" };
  if (compareInt64(first.snapshot_id, id) !== 0) return { state: "expired" };
  return { state: "found", time: first.snapshot_time };
}

/**
 * A snapshot id that can say WHEN it is.
 *
 * A bare 480821 places nothing in time, and every column that holds one
 * (head, the expiry floor, a file's begin_snapshot, a partition's last
 * write) is read as a question about recency. So the id keeps its digits —
 * an id is an identifier, never humanized — and hovering it shows the
 * commit time in UTC plus how long ago that was.
 *
 * POINTER-ONLY, deliberately, and the limit is worth stating plainly: the
 * commit time in this tooltip has NO keyboard path. The trigger is not
 * focusable and carries no native `title`, so a keyboard or screen-reader
 * user cannot reach it here at all. The only non-pointer route to an
 * exact commit instant in the console is the catalog page's snapshot
 * list, which prints `snapshot_time` as its own column — and that covers
 * only catalog-level snapshots, only those inside the page of the list
 * currently loaded. For a file's `begin_snapshot` or a table's
 * EARLIEST_SNAPSHOT there is no substitute at all.
 *
 * The trade: a namespace listing of 200 tables would otherwise gain 200
 * tab stops that do nothing but open a tooltip, interleaved with the
 * page's real controls, which is the worse outcome for the keyboard user
 * this would be for. If the time has to be reachable without a pointer,
 * it needs its own affordance rather than a focusable value.
 *
 * Escape still dismisses an open tooltip (WCAG 1.4.13), through a
 * document-level listener rather than a focused element.
 *
 * The lookup is LAZY, dwell-gated and cached: a listing can paint
 * hundreds of ids and none of them is worth a request until someone
 * stops on one. The query cache (keyed per catalog+id, immutable so never
 * restaled) dedupes two rows showing the same id and a second hover of
 * the same one.
 *
 * `time` short-circuits the fetch for callers that already hold the
 * instant — the snapshot timeline prints `snapshot_time` in the next
 * column — and SEEDS the cache, so the catalog page's header id, which is
 * by definition the timeline's top row, costs nothing to hover either.
 */
export function SnapshotId({
  catalog,
  id,
  time,
}: {
  catalog: string;
  id: Int64;
  /** Known commit time; when set, nothing is fetched. */
  time?: string;
}) {
  // `armed` is sticky once the dwell elapses: closing the tooltip must
  // not cancel a lookup already in flight, and reopening must not re-ask.
  const [armed, setArmed] = useState(false);
  const [open, setOpen] = useState(false);
  // Anchored to the trigger's right edge when it sits in the right half
  // of the viewport: the files table's begin_snapshot is its LAST column,
  // and a left-anchored nowrap tooltip there widens the document.
  const [anchorRight, setAnchorRight] = useState(false);
  const tipId = useId();
  const trigger = useRef<HTMLSpanElement>(null);
  const dwellTimer = useRef<number | undefined>(undefined);
  const closeTimer = useRef<number | undefined>(undefined);
  const queryClient = useQueryClient();

  // Compared NUMERICALLY, not textually: `?snapshot=000` passes
  // isInt64String and is the same sentinel, and probing it would put
  // `after=-1` on the wire and report a retention event that never
  // happened. Every id reaching here is a decimal digit string — the
  // int64 reviver emits canonical ones and the one operator-typed id is
  // validated by isInt64String — so the BigInt parse cannot throw.
  const sentinel = compareInt64(id, NEVER_COMMITTED) === 0;

  const query = useQuery({
    queryKey: snapshotTimeKey(catalog, id),
    queryFn: () => lookupSnapshotTime(catalog, id),
    enabled: armed && time === undefined && !sentinel,
    // A snapshot's commit time is immutable, so a resolved id is never
    // stale. gcTime outlives any plausible visit to one page.
    staleTime: Infinity,
    gcTime: 60 * 60 * 1000,
    // No retry: the trigger for this request is a gesture the reader can
    // repeat, and a tooltip that says it failed is better than one that
    // spins twice behind a pointer that has already moved on.
    retry: false,
  });

  // A caller that already holds the instant shares it with every other
  // id in the page through the cache, rather than keeping it to itself.
  useEffect(() => {
    if (time === undefined) return;
    const known: Lookup = { state: "found", time };
    queryClient.setQueryData(snapshotTimeKey(catalog, id), known);
  }, [queryClient, catalog, id, time]);

  // A row can unmount with a timer pending — changing the snapshot
  // re-renders the whole files table — and a timer that outlives its
  // component sets state on nothing.
  useEffect(
    () => () => {
      window.clearTimeout(dwellTimer.current);
      window.clearTimeout(closeTimer.current);
    },
    [],
  );

  // Escape dismisses a tooltip opened by the POINTER too, which a
  // keydown handler on the trigger cannot do — the trigger never has
  // focus. Bound only while one is open.
  useEffect(() => {
    if (!open) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        // Only the close timer can be pending while open: show() returns
        // early once open, so no dwell is ever queued against an open box.
        window.clearTimeout(closeTimer.current);
        setOpen(false);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [open]);

  const resolved: Lookup | undefined =
    time !== undefined ? { state: "found", time } : query.data;
  const knownTime = resolved?.state === "found" ? resolved.time : undefined;

  const show = () => {
    // A re-entry that beat the close delay: keep what is already open, and
    // do not queue a second dwell for it.
    window.clearTimeout(closeTimer.current);
    if (open) return;
    window.clearTimeout(dwellTimer.current);
    dwellTimer.current = window.setTimeout(() => {
      // Measured here rather than on entry: this is the moment the box is
      // placed, and it is the only moment its side can matter.
      const rect = trigger.current?.getBoundingClientRect();
      if (rect) {
        setAnchorRight(rect.left + rect.width / 2 > window.innerWidth / 2);
      }
      setArmed(true);
      setOpen(true);
    }, DWELL_MS);
  };
  const hide = () => {
    window.clearTimeout(dwellTimer.current);
    window.clearTimeout(closeTimer.current);
    closeTimer.current = window.setTimeout(() => setOpen(false), CLOSE_DELAY_MS);
  };

  let body: ReactNode;
  if (knownTime !== undefined) {
    body = (
      <>
        <span className="snapshot-tooltip-time">{formatTime(knownTime)}</span>
        <span className="snapshot-tooltip-age">
          {formatRelativeAge(knownTime)}
        </span>
      </>
    );
  } else if (sentinel) {
    body = "no snapshot yet";
  } else if (resolved?.state === "expired") {
    body = "expired: below the catalog's retention floor";
  } else if (resolved?.state === "absent") {
    body = "not found (above head)";
  } else if (query.isError) {
    body = "could not look up snapshot";
  } else {
    body = "looking up snapshot…";
  }

  return (
    // Enter/leave sit on the WRAPPER, which contains the tooltip: moving
    // the pointer into the tooltip then never counts as leaving the id.
    <span className="snapshot-id" onMouseEnter={show} onMouseLeave={hide}>
      {/* A plain span: this is a value, and a value that explains itself
          is still a value — not a control, and not a tab stop. */}
      <span
        ref={trigger}
        className="snapshot-id-value"
        // Names the id for page-level assertions and for anyone styling
        // or scripting against one.
        data-snapshot-id={id}
        aria-describedby={open ? tipId : undefined}
      >
        {id}
      </span>
      {open && (
        <span
          id={tipId}
          role="tooltip"
          className={
            anchorRight
              ? "snapshot-tooltip snapshot-tooltip-right"
              : "snapshot-tooltip"
          }
        >
          {body}
        </span>
      )}
    </span>
  );
}
