---
hoglake: patch (Server)
---

A sorted compaction whose spill budget refused a spill file while it was being closed (writing its last row group or its footer) leaked that file's descriptor: the spill directory was removed, but the unlinked file stayed open until the process exited. The writer now closes its stream on that path too.
