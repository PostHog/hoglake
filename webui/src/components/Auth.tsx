import { useEffect, useRef, useState, useSyncExternalStore, type FormEvent } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ApiError } from "../api/client";
import {
  EXPIRY_WARNING_SECONDS,
  currentToken,
  dismissSignIn,
  getAuthState,
  openSignIn,
  secondsLeft,
  signIn,
  signOut,
  subscribeAuth,
  type AuthState,
} from "../lib/auth";
import { formatTime } from "../lib/format";

export function useAuth(): AuthState {
  return useSyncExternalStore(subscribeAuth, getAuthState);
}

/** Re-render on an interval while [active], for countdowns. */
function useNow(active: boolean, everyMs = 15_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), everyMs);
    return () => clearInterval(id);
  }, [active, everyMs]);
  return now;
}

function formatLeft(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

/**
 * Drop every cached response when the credential changes: what one token
 * could see is not what the next (or none) can, and leaving it on screen
 * would show data the new caller was never served.
 */
function useResetOnCredentialChange(token: string | null) {
  const queryClient = useQueryClient();
  const seen = useRef(token);
  useEffect(() => {
    if (token === seen.current) return;
    seen.current = token;
    void queryClient.resetQueries();
  }, [token, queryClient]);
}

/**
 * The header's sign-in state. Signed out it is one quiet control, which is
 * all a server in `off` mode ever shows. Signed in it names the token's
 * subject and time left — decoded in the browser for display, never verified
 * here — and offers sign-out.
 */
export function AuthStatus() {
  const auth = useAuth();
  useResetOnCredentialChange(auth.token);
  const left = secondsLeft(auth.claims, useNow(auth.token !== null, 5_000));
  useEffect(() => {
    // Drops the token the moment it lapses, rather than at the next request.
    if (left !== undefined && left <= 0) currentToken();
  }, [left]);

  if (auth.token === null) {
    return (
      <button
        type="button"
        className="theme-toggle auth-button"
        onClick={openSignIn}
        title="Sign in with a hoglake access token"
      >
        sign in
      </button>
    );
  }
  const c = auth.claims;
  const warn = left !== undefined && left <= EXPIRY_WARNING_SECONDS;
  const title = [
    "Claims decoded in this browser for display only; the server verifies the token.",
    c?.sub && `Subject: ${c.sub}`,
    c?.ten && `Kind: ${c.ten}`,
    c?.cli && `Client: ${c.cli}`,
    c?.catalogs !== undefined && `Catalogs: ${c.catalogs}`,
    c?.iss && `Issuer: ${c.iss}`,
    c?.exp !== undefined && `Expires: ${formatTime(new Date(c.exp * 1000).toISOString())}`,
  ]
    .filter(Boolean)
    .join("\n");
  return (
    <span className="auth-status">
      <span className={warn ? "auth-who auth-expiring" : "auth-who"} title={title}>
        {c?.sub ?? "signed in"}
        {left !== undefined && (
          <span className="auth-left">
            {warn ? ` · expires in ${formatLeft(Math.max(left, 0))}` : ` · ${formatLeft(left)} left`}
          </span>
        )}
      </span>
      <button type="button" className="theme-toggle auth-button" onClick={signOut}>
        sign out
      </button>
    </span>
  );
}

function noticeText(auth: AuthState): string | null {
  switch (auth.notice) {
    case "expired":
      return "Your access token expired. Paste a new one to continue.";
    case "missing":
      return "This server needs an access token for that request.";
    case "rejected":
      // The server saw no credential even though one was sent: something in
      // between dropped the header.
      if (auth.reason === "invalid_request") {
        return (
          "The server did not receive the token (invalid_request). A proxy in " +
          "front of it may be dropping the Authorization header."
        );
      }
      return `The server refused the token${auth.reason ? ` (${auth.reason})` : ""}. Paste a different one.`;
    default:
      return null;
  }
}

/**
 * The sign-in step: paste an access token. Opened from the header, or by the
 * first 401 (src/lib/auth.ts says why it cannot open sooner).
 */
export function SignInPanel() {
  const auth = useAuth();
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  if (!auth.prompt) return null;

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const refusal = signIn(value);
    setError(refusal);
    if (refusal === null) setValue("");
  };
  const cancel = () => {
    setValue("");
    setError(null);
    dismissSignIn();
  };
  const notice = noticeText(auth);
  return (
    <section className="inline-form sign-in-panel" aria-label="Sign in">
      <form onSubmit={submit}>
        {notice && (
          <p className="sign-in-notice" role="alert">
            {notice}
          </p>
        )}
        <p className="subtle sign-in-help">
          Paste a hoglake access token (a JWT). Create a hoglake credential in
          PostHog (Data ops → Settings), then exchange its client id and secret
          at hogtower's <span className="mono">/oauth/token</span> (client
          credentials). The token stays in this tab's session storage and is
          sent only to this server, as an Authorization header.
        </p>
        <div className="form-row">
          <label>
            access token
            <input
              type="password"
              className="sign-in-token"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              autoComplete="off"
              spellCheck={false}
              autoFocus
            />
          </label>
          <button type="submit">Sign in</button>
          <button type="button" className="ghost" onClick={cancel}>
            Cancel
          </button>
          {error && <p className="field-error">{error}</p>}
        </div>
      </form>
    </section>
  );
}

/**
 * A refusal of an operator-only route: 401 (no token), 403 (a token that is
 * not an operator's), or a typed 404. A BARE 404 is not one: that is a server
 * predating the route (see isLedgerUnavailable).
 */
export function isOperatorRefusal(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    (error.status === 401 ||
      error.status === 403 ||
      (error.status === 404 && error.detail !== undefined))
  );
}

/** What an operator-only page shows instead of a broken one. */
export function OperatorTokenNotice({ routes }: { routes: string }) {
  const auth = useAuth();
  return (
    <div className="notice-banner operator-notice" role="status">
      This page needs an operator token: it reads {routes}, which only a
      PostHog operator token may call.{" "}
      {auth.token === null ? (
        <>
          <button type="button" className="theme-toggle auth-button" onClick={openSignIn}>
            sign in
          </button>{" "}
          with one to see it.
        </>
      ) : (
        "The token you signed in with is not one."
      )}
    </div>
  );
}
