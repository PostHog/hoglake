import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

// A stylesheet with an unclosed rule does not fail `tsc -b` or `vite build`:
// the parser folds every subsequent rule into the unterminated block and the
// page renders unstyled. That shipped once — a merge resolution dropped the
// closing brace of `.detail-row .data-table`, which silently swallowed the
// whole database-health block below it.
// The jsdom environment gives import.meta.url a non-file scheme, so resolve
// from the project root vitest runs in rather than from this module.
const css = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");

// Braces inside comments and strings would confuse a naive count; strip both
// before balancing so the check measures the rules themselves.
const rules = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/g, "");

describe("styles.css", () => {
  it("closes every rule it opens", () => {
    const open = (rules.match(/\{/g) ?? []).length;
    const close = (rules.match(/\}/g) ?? []).length;
    expect(close, `${open} rules opened, ${close} closed`).toBe(open);
  });

  it("never nests a bare selector inside another rule", () => {
    // Depth > 1 is legal under @media/@supports/@keyframes, so track which
    // block we are inside: a selector at depth 1 whose enclosing block is a
    // plain rule (not an at-rule) means a closing brace went missing.
    let depth = 0;
    const atRuleDepths: number[] = [];
    let line = 0;
    for (const raw of rules.split("\n")) {
      line += 1;
      const text = raw.trim();
      if (text.startsWith("@") && text.includes("{")) atRuleDepths.push(depth);
      for (const ch of raw) {
        if (ch === "{") depth += 1;
        else if (ch === "}") {
          depth -= 1;
          if (atRuleDepths.at(-1) === depth) atRuleDepths.pop();
        }
      }
      const insideAtRule = atRuleDepths.length > 0;
      const maxDepth = insideAtRule ? 2 : 1;
      expect(
        depth,
        `line ${line} reaches nesting depth ${depth} (max ${maxDepth} here): ${text}`,
      ).toBeLessThanOrEqual(maxDepth);
    }
    expect(depth, "file ends inside an open rule").toBe(0);
  });
});
