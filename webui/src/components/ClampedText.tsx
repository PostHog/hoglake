import { useState } from "react";
import { CopyButton } from "./CopyButton";

/**
 * Text clamped to its first line, with the rest one click away.
 *
 * Two fields in this app carry text that is short in the common case and
 * enormous in the tail: a table comment (up to 16k chars) and a snapshot
 * message, whose millpond form is a one-line key=value summary followed
 * by an `offsets …` block that reaches 16 KiB. Both want the same shape —
 * show the head, keep the row's height, offer the whole thing — so both
 * use this.
 *
 * The clamp is the FIRST NEWLINE first and a character cap second: a
 * two-line message must collapse to its summary line however short that
 * line is, and a single long line still needs a cap because CSS ellipsis
 * alone leaves no way to ask for the rest.
 *
 * Text content only — comments and messages are user data, never HTML.
 */
export function ClampedText({
  text,
  className,
  maxChars,
  expandLabel,
  copyLabel,
  block,
}: {
  text: string;
  /** Class on the wrapper, so each caller keeps its own width + wrapping. */
  className?: string;
  /** Cap on the collapsed head; without one only a newline clamps. */
  maxChars?: number;
  /** Collapsed control's label. Defaults to the plain "more". */
  expandLabel?: string;
  /** When set, the expanded form offers a copy of the WHOLE text. */
  copyLabel?: string;
  /** Render the expanded text as a monospace, wrapped block. */
  block?: boolean;
}) {
  const [open, setOpen] = useState(false);

  const newline = text.indexOf("\n");
  const firstLine = newline === -1 ? text : text.slice(0, newline);
  const capped = maxChars !== undefined && firstLine.length > maxChars;
  const head = capped ? `${firstLine.slice(0, maxChars)}…` : firstLine;
  // A trailing newline is not a second line: expanding it would reveal
  // nothing, so it must not earn a control.
  const hasRest = newline !== -1 && text.slice(newline + 1).trim() !== "";
  // Nothing hidden: render exactly what the caller passed, with no control
  // to press. A compaction message stays the bare line it always was.
  if (!hasRest && !capped) {
    return <span className={className}>{text}</span>;
  }

  return (
    <span className={className}>
      {open ? (
        // A span rather than <pre>: this renders inside inline wrappers
        // (a <dd>, a table cell's span) where a block element would be
        // invalid nesting. `.clamped-block` supplies the pre-wrap.
        block ? (
          <span className="clamped-block mono">{text}</span>
        ) : (
          text
        )
      ) : (
        <span className="clamped-head">{head}</span>
      )}{" "}
      <button
        type="button"
        className="ghost clamp-toggle"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        {open ? "less" : (expandLabel ?? "more")}
      </button>
      {open && copyLabel !== undefined && (
        <CopyButton text={text} label={copyLabel} />
      )}
    </span>
  );
}
