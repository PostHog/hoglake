---
hoglake: patch (Server)
---

The hydrator emits `hoglake_stats_hydrated_total` and its per-file outcome lines only after the sweep commits (#610, fixes #488), so a rolled-back sweep no longer counts files that stayed `pending`. The transient object-store counter and warning, and the "could not mark failed" error, stay immediate.
