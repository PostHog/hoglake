---
hoglake: patch (Server)
---

Hoglite: a width-changing promote (`int` to `long`, `float` to `double`) fences on the promoted table's files, not on the catalog-wide file allocator, so appends to other tables no longer replan it or answer 409 on a busy catalog. Appends to the promoted table itself replan it up to 3 times, on a budget of its own instead of the one replan it shared with a concurrent schema change. A blind append whose table's schema changes while it is planned now replans up to 3 times, on a budget of its own instead of the one replan it shared with receipt recovery. The 4th fence trip in either case still answers 409 `commit_conflict`, where PostgreSQL waits on its lock; the README's Hoglite backend section states it. (#687)
