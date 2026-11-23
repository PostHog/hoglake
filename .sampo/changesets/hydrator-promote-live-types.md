---
hoglake: patch (Server)
---

A file without field ids that is still `pending` when its column is promoted from `int` to `long` or `float` to `double` now hydrates with 8-byte bounds in the promoted type. It binds by name to the columns of its begin snapshot and used to take their old types as well, so its 4-byte bounds landed under the live `long` or `double`, which readers decode as 8 bytes and which the promote, having run first, did not re-encode. Split out of #723 (#675); the Hoglite hydrator there applies the same binding.
