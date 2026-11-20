---
hoglake: patch (Server)
---

Sorted compaction checks decoded rows, readers, and writers against its memory budgets.
The worker remembers deterministic memory and spill-budget refusals for unchanged groups.
Occasional large rows do not force a row group flush by themselves.
The maintenance API reports `oversized_row_groups` as a subset of `merge_budget_exceeded`.
The console shows this subset without counting it twice.
