// The bearer token the console presents to a server running with auth in
// `warn` or `enforce` (docs/auth-tenancy.md §8.6).
//
// The token is PASTED: a hoglake access token (a JWT) the user obtained
// from hogtower's /oauth/token with a credential created in PostHog. It is
// held in sessionStorage and nowhere else — not localStorage (it would
// outlive the tab), not a cookie (it would ride every request the browser
// makes to this origin, the static assets included), never a URL (it would
// land in history and access logs). sessionStorage is readable by any
// script on the page; that is accepted because the token is short-lived.
// A server-side session that keeps the token out of JS is the later option.
//
// Nothing here logs the token, and the claims decoded below are for
// DISPLAY only: the browser verifies nothing, the server verifies every
// request.
//
// /v1/info says nothing about the auth mode, so the console cannot tell
// `off` from `warn` up front. It never prompts on its own: the sign-in
// panel opens on the first 401 (only `enforce` sends one to an anonymous
// caller), and a quiet "sign in" control is always in the header.

const STORAGE_KEY = "hoglake-access-token";

/** The header starts warning this long before the token's `exp`. */
export const EXPIRY_WARNING_SECONDS = 120;

/** Claims read for display. Every field is optional: none is required to show the state. */
export interface TokenClaims {
  sub?: string;
  iss?: string;
  exp?: number;
  iat?: number;
  /** Token kind: tenant, scoped, operator (docs/auth-tenancy.md §6.2a). */
  ten?: string;
  /** The issuing client's display name. */
  cli?: string;
  /** How many catalogs the `cat` claim names. */
  catalogs?: number;
}

/**
 * Why the sign-in panel is open, when something other than the user opened it.
 * `missing`: a request without a token got a 401. `expired`: the held token
 * passed its `exp`, or the server said `token_expired`. `rejected`: the server
 * refused the held token for another reason, carried in [AuthState.reason].
 */
export type SignInNotice = "missing" | "expired" | "rejected";

export interface AuthState {
  token: string | null;
  claims: TokenClaims | null;
  /** Whether the sign-in panel is showing. */
  prompt: boolean;
  notice: SignInNotice | null;
  /** The server's closed reason code (RFC 6750 `error`) for a `rejected` notice. */
  reason?: string;
  /**
   * The user closed the panel without a token: a further anonymous 401 does
   * not reopen it. Without this, every poll on an `enforce` server would pop
   * it straight back up.
   */
  dismissed: boolean;
}

const JWS = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;

function base64UrlJson(segment: string): unknown {
  const b64 = segment.replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}

/**
 * The payload of a compact JWS, decoded WITHOUT verification, or null when
 * the string is not shaped like one. For display only.
 */
export function decodeClaims(token: string): TokenClaims | null {
  if (!JWS.test(token)) return null;
  let payload: unknown;
  try {
    payload = base64UrlJson(token.split(".")[1]);
  } catch {
    return null;
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
  const p = payload as Record<string, unknown>;
  const str = (v: unknown) => (typeof v === "string" ? v : undefined);
  const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : undefined);
  const cat = p.cat;
  return {
    sub: str(p.sub),
    iss: str(p.iss),
    exp: num(p.exp),
    iat: num(p.iat),
    ten: str(p.ten),
    cli: str(p.cli),
    catalogs:
      cat && typeof cat === "object" && !Array.isArray(cat)
        ? Object.keys(cat).length
        : undefined,
  };
}

/** Seconds until the claims' `exp`, or undefined when the token carries none. */
export function secondsLeft(claims: TokenClaims | null, now = Date.now()): number | undefined {
  if (claims?.exp === undefined) return undefined;
  return Math.floor(claims.exp - now / 1000);
}

function isExpired(claims: TokenClaims | null, now = Date.now()): boolean {
  const left = secondsLeft(claims, now);
  return left !== undefined && left <= 0;
}

function storage(): Storage | undefined {
  try {
    return window.sessionStorage ?? undefined;
  } catch {
    // Storage disabled (some privacy modes): the token lives in memory only.
    return undefined;
  }
}

function persist(token: string | null) {
  try {
    if (token === null) storage()?.removeItem(STORAGE_KEY);
    else storage()?.setItem(STORAGE_KEY, token);
  } catch {
    // Quota or a disabled store: memory still holds it for this page.
  }
}

const SIGNED_OUT: AuthState = {
  token: null,
  claims: null,
  prompt: false,
  notice: null,
  dismissed: false,
};

function load(): AuthState {
  let token: string | null = null;
  try {
    token = storage()?.getItem(STORAGE_KEY) ?? null;
  } catch {
    token = null;
  }
  if (token === null) return SIGNED_OUT;
  const claims = decodeClaims(token);
  if (claims === null) {
    persist(null);
    return SIGNED_OUT;
  }
  if (isExpired(claims)) {
    // A reload after the token lapsed: say so rather than just looking signed out.
    persist(null);
    return { ...SIGNED_OUT, prompt: true, notice: "expired" };
  }
  return { ...SIGNED_OUT, token, claims };
}

let state: AuthState = load();
const listeners = new Set<() => void>();

function set(next: AuthState) {
  state = next;
  for (const listener of listeners) listener();
}

export function subscribeAuth(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getAuthState(): AuthState {
  return state;
}

/**
 * The token to send on a request, or null. A held token past its `exp` is
 * dropped here, so a server in `warn` (which lets an expired token through as
 * anonymous rather than 401-ing it) still gets the user back to the panel.
 */
export function currentToken(now = Date.now()): string | null {
  if (state.token !== null && isExpired(state.claims, now)) {
    persist(null);
    set({ ...SIGNED_OUT, prompt: true, notice: "expired" });
    return null;
  }
  return state.token;
}

/**
 * Hold [raw] as the token. Returns a message for the form when it is refused
 * here (not shaped like a JWT, or already expired), else null. The message
 * never quotes the input.
 */
export function signIn(raw: string, now = Date.now()): string | null {
  // Tolerate a pasted header value or the token with stray whitespace.
  const token = raw.trim().replace(/^bearer\s+/i, "").trim();
  if (token === "") return "Paste an access token.";
  const claims = decodeClaims(token);
  if (claims === null) {
    return "That is not a JWT: expected three base64url parts separated by dots.";
  }
  if (isExpired(claims, now)) return "That token has already expired. Request a new one.";
  persist(token);
  set({ ...SIGNED_OUT, token, claims });
  return null;
}

export function signOut() {
  persist(null);
  // Counts as a dismissal: signing out is not a request to be asked again.
  set({ ...SIGNED_OUT, dismissed: true });
}

export function openSignIn() {
  set({ ...state, prompt: true });
}

export function dismissSignIn() {
  set({ ...state, prompt: false, notice: null, reason: undefined, dismissed: true });
}

/** The `error` parameter of a `WWW-Authenticate: Bearer error="..."` challenge. */
export function challengeError(header: string | null): string | undefined {
  if (!header) return undefined;
  const m = /\berror="([^"]*)"/i.exec(header);
  return m ? m[1] : undefined;
}

/**
 * React to a 401. [sent] is the token the failed request carried (null when
 * none), so a slow request that started before a sign-in cannot sign out the
 * token that replaced it. [reason] is the server's closed reason code.
 */
export function unauthorized(sent: string | null, reason: string | undefined) {
  if (sent !== null) {
    if (state.token !== sent) return;
    persist(null);
    set({
      ...SIGNED_OUT,
      prompt: true,
      notice: reason === "token_expired" ? "expired" : "rejected",
      reason,
    });
    return;
  }
  // Anonymous. Ignored when a token is now held (the request predates it),
  // when the panel is already up, or when the user has closed it.
  if (state.token !== null || state.prompt || state.dismissed) return;
  set({ ...state, prompt: true, notice: "missing", reason });
}

/** Test seam: forget everything, in memory and in storage. */
export function resetAuthForTests() {
  persist(null);
  set(SIGNED_OUT);
}
