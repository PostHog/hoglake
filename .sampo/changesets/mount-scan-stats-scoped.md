---
hoglake: patch (Server)
---

A mount scan with `include=column_stats` reads the statistics of its own scoped files only, instead of every `provided` file of the pinned table (#648). On a large shared table the old read could hit the statement deadline or fill the heap. Rows returned, the entry cap and the `provided`-only rule are unchanged.
