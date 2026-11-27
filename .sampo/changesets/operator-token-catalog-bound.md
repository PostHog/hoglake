---
hoglake: patch (Server)
---

Bind an operator token that carries `cat` to the catalogs it names, as hoglite does: other catalogs answer 404, `POST /v1/catalogs` creates only a named catalog (any other name, reserved or not, is a 404, and a `data_path` overlap names the other catalog only when the token names it too), the listing shows its own, and the fleet routes answer 403. The binding must grant `admin` on every catalog it names, as hoglite requires; a narrower word refuses the token as malformed. An operator token without `cat` or `org` stays fleet-wide; one carrying `org` with no `cat` is refused as malformed rather than read as fleet-wide with its org dropped, as hoglite refuses it. An operator token carrying a `scope` claim, whatever its value, is now refused as malformed, as hoglite refuses it; it was previously accepted with the `scope` ignored.
