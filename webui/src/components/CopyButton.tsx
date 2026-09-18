import { useEffect, useRef, useState } from "react";

/**
 * Icon-only copy button.
 *
 * An icon rather than the word "copy" because these sit beside values —
 * paths, uuids — that already fill their column, and a text button
 * competes with the thing it exists to copy. The label survives as the
 * tooltip and the accessible name, so nothing is lost by dropping the
 * visible word.
 */
export function CopyButton({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  // A row can unmount while the confirmation is still showing — changing
  // the snapshot re-renders the whole files table — and a timer that
  // outlives its component sets state on nothing.
  useEffect(() => () => window.clearTimeout(timer.current), []);

  const what = label ?? "value";
  return (
    <button
      type="button"
      className={`copy-btn${copied ? " copy-btn-done" : ""}`}
      title={copied ? `Copied ${what}` : `Copy ${what}`}
      aria-label={`Copy ${what}`}
      onClick={() => {
        if (!navigator.clipboard) return;
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.clearTimeout(timer.current);
          timer.current = window.setTimeout(() => setCopied(false), 1200);
        });
      }}
    >
      {copied ? <CheckIcon /> : <ClipboardIcon />}
    </button>
  );
}

/*
 * Inline SVG rather than a glyph: a clipboard emoji renders at the
 * font's mercy and in its own colour, which is not what a dim row action
 * should do. currentColor keeps both icons on whatever theme is active.
 */
function ClipboardIcon() {
  return (
    <svg
      viewBox="0 0 16 16"
      width="12"
      height="12"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.3"
      aria-hidden="true"
      focusable="false"
    >
      <rect x="5.5" y="1.5" width="5" height="2.5" rx="0.75" />
      <path d="M10.5 2.75h1.75a1 1 0 0 1 1 1v9.75a1 1 0 0 1-1 1h-8.5a1 1 0 0 1-1-1V3.75a1 1 0 0 1 1-1H5.5" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg
      viewBox="0 0 16 16"
      width="12"
      height="12"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M3 8.5l3.5 3.5L13 5" />
    </svg>
  );
}
