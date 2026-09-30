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

/**
 * Two rules whose sizes are load-bearing: the files table's stats column
 * holds either an expander button or a "no stats" marker, and the code's
 * own comment promises they are the same box so the column cannot jump
 * between rows. jsdom applies no stylesheet, so a DOM test can never see
 * this — the sheet itself is the only place to assert it.
 */
function declarations(selector: string): Record<string, string> {
  const at = rules.indexOf(`${selector} {`);
  if (at === -1) throw new Error(`no rule for ${selector}`);
  const body = rules.slice(at + selector.length + 2, rules.indexOf("}", at));
  const out: Record<string, string> = {};
  for (const line of body.split(";")) {
    const [prop, ...rest] = line.split(":");
    if (rest.length === 0) continue;
    out[prop.trim()] = rest.join(":").trim();
  }
  return out;
}

describe("files tab stats column", () => {
  it("sizes the expander to a 24x24 hit area", () => {
    // The control was a bare 13px glyph and people missed it; 24px is the
    // smallest pointer target anyone recommends.
    const toggle = declarations("button.expand-toggle");
    expect(toggle.width).toBe("24px");
    expect(toggle.height).toBe("24px");
    // And the glyph inside it is bigger than the body text.
    expect(declarations(".expand-chevron")["font-size"]).toBe("16px");
  });

  it("gives the stats marker the identical box, so the column cannot jump", () => {
    const toggle = declarations("button.expand-toggle");
    const marker = declarations(".stats-marker");
    expect(marker.width).toBe(toggle.width);
    expect(marker.height).toBe(toggle.height);
  });

  it("rotates the chevron instead of swapping a character", () => {
    // A swapped glyph cannot animate and can change the box's metrics.
    // Read from the raw sheet: `rules` has its string literals stripped,
    // which takes the attribute selector's value with them.
    expect(css).toMatch(
      /button\.expand-toggle\[aria-expanded="true"\] \.expand-chevron \{[^}]*transform: rotate\(90deg\)/,
    );
  });
});
