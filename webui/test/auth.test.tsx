import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchMetricsText, listCatalogs } from "../src/api/client";
import {
  challengeError,
  currentToken,
  decodeClaims,
  dismissSignIn,
  getAuthState,
  signIn,
  unauthorized,
} from "../src/lib/auth";
import { catalogsFixture } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

function b64url(value: unknown): string {
  return btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** An unsigned-looking compact JWS: the console never verifies, so the signature is filler. */
function jwt(claims: Record<string, unknown>): string {
  return `${b64url({ alg: "ES256", typ: "at+jwt" })}.${b64url(claims)}.c2ln`;
}

const nowS = () => Math.floor(Date.now() / 1000);
const tenantToken = (exp = nowS() + 900) =>
  jwt({ sub: "client:abc", ten: "tenant", cli: "console", exp, cat: { analytics: ["read"] } });

function headersOf(call: unknown[]): Record<string, string> {
  return ((call[1] as RequestInit | undefined)?.headers ?? {}) as Record<string, string>;
}

function unauthorizedResponse(reason: string): Response {
  return new Response(JSON.stringify({ error: reason, detail: "token refused" }), {
    status: 401,
    headers: { "Content-Type": "application/json", "WWW-Authenticate": `Bearer error="${reason}"` },
  });
}

afterEach(() => {
  vi.useRealTimers();
});

describe("token handling", () => {
  it("decodes display claims without verifying, and rejects what is not a JWS", () => {
    const claims = decodeClaims(tenantToken(2_000_000_000));
    expect(claims).toMatchObject({ sub: "client:abc", ten: "tenant", cli: "console", exp: 2_000_000_000, catalogs: 1 });
    expect(decodeClaims("not-a-token")).toBeNull();
    expect(decodeClaims("a.b.c")).toBeNull();
  });

  it("keeps the token in sessionStorage only, and refuses an expired paste", () => {
    expect(signIn("garbage")).toMatch(/not a JWT/);
    expect(signIn(tenantToken(nowS() - 10))).toMatch(/already expired/);
    expect(getAuthState().token).toBeNull();

    const token = tenantToken();
    expect(signIn(`Bearer ${token}\n`)).toBeNull();
    expect(getAuthState().token).toBe(token);
    expect(window.sessionStorage.getItem("hoglake-access-token")).toBe(token);
    expect(window.localStorage.getItem("hoglake-access-token")).toBeNull();
    expect(document.cookie).not.toContain(token);
  });

  it("reads the RFC 6750 error code from the challenge", () => {
    expect(challengeError('Bearer error="token_expired"')).toBe("token_expired");
    expect(challengeError("Bearer")).toBeUndefined();
    expect(challengeError(null)).toBeUndefined();
  });
});

describe("api client with a token", () => {
  it("sends no Authorization header when signed out (the auth-off path)", async () => {
    const fetchMock = mockFetch(() => jsonResponse(catalogsFixture));
    await listCatalogs();
    expect(headersOf(fetchMock.mock.calls[0])).toEqual({ Accept: "application/json" });
  });

  it("adds Authorization: Bearer to API calls and /metrics", async () => {
    const token = tenantToken();
    signIn(token);
    const fetchMock = mockFetch((url) =>
      url === "/metrics" ? new Response("# none\n") : jsonResponse(catalogsFixture),
    );
    await listCatalogs();
    await fetchMetricsText();
    for (const call of fetchMock.mock.calls) {
      expect(headersOf(call).Authorization).toBe(`Bearer ${token}`);
    }
    expect(headersOf(fetchMock.mock.calls[0]).Accept).toBe("application/json");
  });

  it("drops the token on a 401 and says it expired", async () => {
    signIn(tenantToken());
    mockFetch(() => unauthorizedResponse("token_expired"));
    await expect(listCatalogs()).rejects.toMatchObject({ status: 401, error: "token_expired" });
    expect(getAuthState()).toMatchObject({ token: null, prompt: true, notice: "expired" });
    expect(window.sessionStorage.getItem("hoglake-access-token")).toBeNull();
  });

  it("names any other refusal reason, read from the body when no challenge is sent", async () => {
    signIn(tenantToken());
    mockFetch(() => jsonResponse({ error: "invalid_signature" }, 401));
    await expect(listCatalogs()).rejects.toMatchObject({ status: 401 });
    expect(getAuthState()).toMatchObject({ token: null, notice: "rejected", reason: "invalid_signature" });
  });

  it("does not sign out a token that replaced the one a slow 401 carried", () => {
    const first = tenantToken();
    signIn(first);
    const second = tenantToken(nowS() + 600);
    signIn(second);
    unauthorized(first, "token_expired");
    expect(getAuthState()).toMatchObject({ token: second, prompt: false });
  });

  it("prompts once on an anonymous 401 and stays closed after a dismissal", async () => {
    mockFetch(() => unauthorizedResponse("invalid_request"));
    await expect(listCatalogs()).rejects.toMatchObject({ status: 401 });
    expect(getAuthState()).toMatchObject({ prompt: true, notice: "missing" });
    dismissSignIn();
    await expect(listCatalogs()).rejects.toMatchObject({ status: 401 });
    expect(getAuthState().prompt).toBe(false);
  });

  it("stops sending a token once it passes exp, without waiting for the server", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    signIn(tenantToken(nowS() + 30));
    vi.setSystemTime(Date.now() + 31_000);
    const fetchMock = mockFetch(() => jsonResponse([]));
    await listCatalogs();
    expect(headersOf(fetchMock.mock.calls[0]).Authorization).toBeUndefined();
    expect(getAuthState()).toMatchObject({ token: null, prompt: true, notice: "expired" });
    expect(currentToken()).toBeNull();
  });
});

describe("sign-in UI", () => {
  it("shows only a sign-in control when the server never asks (auth off)", async () => {
    const fetchMock = mockFetch((url) => (url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined));
    renderApp("/");
    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "sign in" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Sign in" })).toBeNull();
    for (const call of fetchMock.mock.calls) {
      expect(headersOf(call).Authorization).toBeUndefined();
    }
  });

  it("prompts on a 401, signs in with a pasted token, refetches with it, and signs out", async () => {
    const user = userEvent.setup();
    const token = tenantToken();
    const fetchMock = mockFetch((url, init) => {
      if (url !== "/v1/catalogs") return undefined;
      const auth = (init?.headers as Record<string, string>).Authorization;
      return auth === `Bearer ${token}` ? jsonResponse(catalogsFixture) : unauthorizedResponse("invalid_request");
    });
    renderApp("/");
    const panel = await screen.findByRole("region", { name: "Sign in" });
    expect(within(panel).getByRole("alert")).toHaveTextContent("needs an access token");

    await user.type(within(panel).getByLabelText(/access token/i), "nope");
    await user.click(within(panel).getByRole("button", { name: "Sign in" }));
    expect(within(panel).getByText(/not a JWT/)).toBeInTheDocument();

    await user.clear(within(panel).getByLabelText(/access token/i));
    await user.type(within(panel).getByLabelText(/access token/i), token);
    await user.click(within(panel).getByRole("button", { name: "Sign in" }));

    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Sign in" })).toBeNull();
    expect(screen.getByText("client:abc")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some((c) => headersOf(c).Authorization === `Bearer ${token}`)).toBe(true);

    await user.click(screen.getByRole("button", { name: "sign out" }));
    expect(getAuthState().token).toBeNull();
    expect(screen.getAllByRole("button", { name: "sign in" }).length).toBeGreaterThan(0);
    // A sign-out is a dismissal: the anonymous 401 that follows does not reopen the panel.
    await screen.findAllByRole("alert");
    expect(screen.queryByRole("region", { name: "Sign in" })).toBeNull();
  });

  it("says when the token was refused as expired", async () => {
    signIn(tenantToken());
    mockFetch((url) => (url === "/v1/catalogs" ? unauthorizedResponse("token_expired") : undefined));
    renderApp("/");
    const panel = await screen.findByRole("region", { name: "Sign in" });
    expect(within(panel).getByRole("alert")).toHaveTextContent("Your access token expired");
  });

  it("warns shortly before expiry", async () => {
    signIn(tenantToken(nowS() + 60));
    mockFetch((url) => (url === "/v1/catalogs" ? jsonResponse([]) : undefined));
    renderApp("/");
    expect(await screen.findByText(/expires in \d+s/)).toBeInTheDocument();
  });
});

describe("operator-only pages", () => {
  function mockOperatorRoutes(status: number, error: string) {
    return mockFetch((url) =>
      url.startsWith("/v1/maintenance/") || url === "/v1/database/health"
        ? status === 401
          ? unauthorizedResponse(error)
          : jsonResponse({ error, detail: "this credential does not hold 'admin'" }, status)
        : undefined,
    );
  }

  it("central maintenance shows the operator-token state for a tenant token's 403, and stops polling", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    signIn(tenantToken());
    const fetchMock = mockOperatorRoutes(403, "forbidden");
    renderApp("/maintenance");
    expect(await screen.findByText(/needs an operator token/)).toHaveTextContent(
      "The token you signed in with is not one.",
    );
    const statusCalls = () => fetchMock.mock.calls.filter((c) => String(c[0]).startsWith("/v1/maintenance/status")).length;
    const before = statusCalls();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(12_000);
    });
    expect(statusCalls()).toBe(before);
    expect(getAuthState().token).not.toBeNull();
  });

  it("central maintenance offers sign-in on an anonymous 401", async () => {
    mockOperatorRoutes(401, "invalid_request");
    renderApp("/maintenance");
    const notice = await screen.findByText(/needs an operator token/);
    expect(within(notice).getByRole("button", { name: "sign in" })).toBeInTheDocument();
    expect(await screen.findByRole("region", { name: "Sign in" })).toBeInTheDocument();
  });

  it("the database page shows the same state", async () => {
    signIn(tenantToken());
    mockOperatorRoutes(403, "forbidden");
    renderApp("/database");
    expect(await screen.findByText(/needs an operator token/)).toHaveTextContent("/v1/database/health");
  });
});
