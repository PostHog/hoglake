---
hoglake: patch (Server)
---

A Hoglite backend's 401 for a token hoglake accepted no longer answers 401 `invalid_token`, which made clients re-mint the token and retry into the same refusal. A token whose `exp` has passed answers 401 `token_expired`; any other answers 403 `backend_refused_authority` with no `WWW-Authenticate` challenge, because a new token with the same claims is refused again; a client may retry the same token after a delay, since a signing key hoglite has not fetched yet and a token issued ahead of hoglite's clock pass with time. The typed scoped reads map hoglite's 401 the same way instead of answering 503 with `Retry-After`. docs/auth.md lists the rules hoglite applies on top of hoglake's, and the contract vectors carry hoglite's verdict as `backend_expect` on every accepted vector routed to it, with a verifier-only vector for each claim rule under which hoglake accepts a token and hoglite refuses it (#681).
