---
hoglake: patch (Server)
---

Bind an operator token that carries `cat`, or the same grants in the standard `scope` claim, to the catalogs it names, as hoglite does: other catalogs answer 404, `POST /v1/catalogs` creates only a named catalog, the listing shows its own, and the fleet routes answer 403. The binding must grant `admin` on every catalog it names, as hoglite requires; a narrower word refuses the token as malformed. An operator token without `cat`, `scope` or `org` stays fleet-wide; one carrying `org` with no `cat` or `scope` is refused as malformed rather than read as fleet-wide with its org dropped, as hoglite refuses it.
