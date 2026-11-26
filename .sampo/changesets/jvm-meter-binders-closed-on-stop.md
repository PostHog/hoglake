---
hoglake: patch (Server)
---

The API module closes its JVM meter binders when its application stops. Ktor's metrics plugin never closed them, and the GC metrics binder's listener on the JVM's garbage collector beans kept every stopped application reachable, with everything it held. A server runs one, so this cost nothing in production; the server test suite starts hundreds, which held about half of the test JVM's heap and ran it out of memory.
