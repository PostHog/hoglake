"""Exercise the extension's bearer authentication against a live auth-enabled server.

Not part of run-live-tests.sh: the main suite needs a server with
HOGLAKE_AUTH_MODE=off, and these cases need one in `enforce` or `warn` that
trusts hogtower's contract key. Start the server with

    HOGLAKE_AUTH_MODE=enforce   (or warn)
    HOGLAKE_AUTH_ISSUER=https://hogtower.contract.test
    HOGLAKE_AUTH_AUDIENCE=hoglake
    HOGLAKE_AUTH_JWKS_FILE=<a file holding the `jwks` object of contract-vectors.json>

then, from a pyhoglake checkout:

    DUCKEXT_AUTH_MODE=enforce HOGLAKE_URL=... DUCKEXT_S3_ENDPOINT=... \\
        uv run --with cryptography python <this file>

DUCKEXT_AUTH_MODE is the gate and must name the server's mode. The signing
key is the contract file's published test key (HOGLAKE_AUTH_VECTORS, default
server/src/test/resources/oauth/contract-vectors.json in this repo).
"""

import base64
import json
import os
import re
import subprocess
import sys
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / "build" / "release"
MODE = os.environ.get("DUCKEXT_AUTH_MODE", "")
URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080").rstrip("/")
S3_ENDPOINT = os.environ.get("DUCKEXT_S3_ENDPOINT", "localhost:9000")
VECTORS = Path(
    os.environ.get(
        "HOGLAKE_AUTH_VECTORS",
        ROOT.parent / "server/src/test/resources/oauth/contract-vectors.json",
    )
)
CLIENT_ID = "hlc_duckext0auth0test0000001"
CLIENT_SECRET = "s3cret-for-the-token-endpoint"
SUBJECT = CLIENT_ID
RUN = uuid.uuid4().hex[:8]
CATALOG = f"duckext-auth-{RUN}"


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_int(text):
    return int.from_bytes(
        base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)), "big"
    )


class Signer:
    """ES256 over the contract key (or any P-256 key), with fresh iat/exp."""

    def __init__(self, key, kid, issuer):
        self.key, self.kid, self.issuer = key, kid, issuer

    def mint(self, claims, lifetime=600):
        now = int(time.time())
        body = {
            "iss": self.issuer,
            "aud": "hoglake",
            "jti": str(uuid.uuid4()),
            "iat": now,
            "nbf": now,
            "exp": now + lifetime,
            **claims,
        }
        header = {"alg": "ES256", "typ": "JWT", "kid": self.kid}
        signing_input = (
            b64url(json.dumps(header).encode())
            + "."
            + b64url(json.dumps(body).encode())
        )
        r, s = decode_dss_signature(
            self.key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256()))
        )
        return (
            signing_input + "." + b64url(r.to_bytes(32, "big") + s.to_bytes(32, "big"))
        )

    def tenant(self, catalog=CATALOG, scopes=("write",), subject=SUBJECT):
        return self.mint(
            {
                "sub": subject,
                "ten": "tenant",
                "org": "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
                "cat": {catalog: list(scopes)},
                "cli": "duckdb-client",
            }
        )

    def operator(self):
        return self.mint(
            {"sub": "hogtower:provisioner", "ten": "operator", "cli": "hogtower"}
        )


def load_signers():
    vectors = json.loads(VECTORS.read_text())
    kid = vectors["jwks"]["keys"][0]["kid"]
    good = ec.derive_private_key(
        b64url_int(vectors["test_private_key_d"]), ec.SECP256R1()
    )
    # same kid, different key: the server finds the key and the signature fails
    wrong = ec.generate_private_key(ec.SECP256R1())
    return Signer(good, kid, vectors["issuer"]), Signer(wrong, kid, vectors["issuer"])


class TokenEndpoint:
    """An RFC 6749 client-credentials endpoint handing out queued tokens."""

    def __init__(self):
        self.tokens = []  # (access_token, expires_in) popped per mint
        self.requests = []
        self.failure = None
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                try:
                    body = self.rfile.read(
                        int(self.headers.get("Content-Length", "0"))
                    ).decode()
                    owner.requests.append(
                        {
                            "path": self.path,
                            "authorization": self.headers.get("Authorization", ""),
                            "content_type": self.headers.get("Content-Type", ""),
                            "user_agent": self.headers.get("User-Agent", ""),
                            "form": {
                                k: v[0]
                                for k, v in parse_qs(
                                    body, keep_blank_values=True
                                ).items()
                            },
                        }
                    )
                    expected = (
                        "Basic "
                        + base64.b64encode(
                            f"{CLIENT_ID}:{CLIENT_SECRET}".encode()
                        ).decode()
                    )
                    form = owner.requests[-1]["form"]
                    if self.headers.get("Authorization") != expected:
                        return self.reply(401, {"error": "invalid_client"})
                    if form != {
                        "grant_type": "client_credentials",
                        "audience": "hoglake",
                    }:
                        return self.reply(400, {"error": "invalid_request"})
                    token, expires_in = owner.tokens.pop(0)
                    self.reply(
                        200,
                        {
                            "access_token": token,
                            "token_type": "Bearer",
                            "expires_in": expires_in,
                        },
                    )
                except Exception as error:  # noqa: BLE001 -- surfaced by the test via owner.failure
                    owner.failure = error
                    self.reply(500, {"error": "server_error"})

            def reply(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/oauth/token"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def auth_metrics(operator_token):
    """hoglake_auth_requests_total samples as {(mode, outcome, client): value}."""
    text = (
        httpx.get(
            URL + "/metrics",
            headers={"Authorization": f"Bearer {operator_token}"},
            timeout=10,
        )
        .raise_for_status()
        .text
    )
    samples = {}
    for line in text.splitlines():
        if not line.startswith("hoglake_auth_requests_total{"):
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', line[: line.index("}")]))
        samples[(labels.get("mode"), labels.get("outcome"), labels.get("client"))] = (
            float(line.rsplit(" ", 1)[1])
        )
    return samples


def delta(before, after, key):
    return after.get(key, 0.0) - before.get(key, 0.0)


class AuthBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signer, cls.wrong_signer = load_signers()
        cls.operator = cls.signer.operator()
        # POST /v1/catalogs is operator-only once auth resolves callers
        response = httpx.post(
            URL + "/v1/catalogs",
            json={"name": CATALOG, "data_path": f"s3://duckext-itest/auth-{RUN}/"},
            headers={"Authorization": f"Bearer {cls.operator}"},
            timeout=30,
        )
        if response.status_code != 201:
            raise AssertionError(
                f"create catalog: {response.status_code} {response.text}"
            )

    def setUp(self):
        self.endpoint = TokenEndpoint()
        self.addCleanup(self.endpoint.close)

    def duckdb(self, attach_options, sql, succeeds=True):
        options = ", ".join([f"ENDPOINT '{URL}'", *attach_options])
        script = f"""
            LOAD '{BUILD / "extension/hoglake/hoglake.duckdb_extension"}';
            LOAD '{BUILD / "extension/httpfs/httpfs.duckdb_extension"}';
            CREATE SECRET (TYPE s3, KEY_ID 'hoglake', SECRET 'hoglake123',
                ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false);
            ATTACH 'hoglake:{CATALOG}' AS lake ({options});
        """
        result = subprocess.run(
            [
                str(BUILD / "duckdb"),
                "-unsigned",
                "-csv",
                "-bail",
                "-no-stdin",
                "-c",
                script + sql,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertIsNone(self.endpoint.failure)
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode == 0, succeeds, output)
        return output

    def round_trip(self, options, schema):
        """A small CREATE/INSERT/SELECT; returns the output."""
        output = self.duckdb(
            options,
            f"""
            CREATE SCHEMA IF NOT EXISTS lake.{schema};
            CREATE TABLE lake.{schema}.t (id BIGINT, v VARCHAR);
            INSERT INTO lake.{schema}.t VALUES (1, 'one'), (2, 'two');
            SELECT 'rows=' || count(*) || ',sum=' || sum(id) FROM lake.{schema}.t;
            """,
        )
        self.assertIn("rows=2,sum=3", output)
        return output

    def authors(self, schema_marker):
        """(author, message) of every snapshot from the one creating `schema_marker` on."""
        output = self.duckdb(
            [f"TOKEN '{self.signer.tenant()}'"],
            "SELECT 'SNAP|' || snapshot_id || '|' || coalesce(author, '<null>') || '|' || coalesce(message, '') "
            "FROM hoglake_snapshots('lake') ORDER BY snapshot_id;",
        )
        rows = [
            line.split("|", 3)[1:]
            for line in output.splitlines()
            if line.startswith("SNAP|")
        ]
        first = next(i for i, row in enumerate(rows) if schema_marker in row[2])
        return [(author, message) for _, author, message in rows[first:]]


@unittest.skipUnless(MODE == "enforce", "server not in enforce mode")
class Enforce(AuthBase):
    def test_a_no_token_is_refused(self):
        before = auth_metrics(self.operator)
        output = self.duckdb([], "SELECT 1;", succeeds=False)
        self.assertRegex(output, r"Permission Error: .*HTTP 401")
        after = auth_metrics(self.operator)
        self.assertGreaterEqual(
            delta(before, after, ("enforce", "missing", "duckdb")), 1
        )

    def test_a_fixed_token_401_is_final(self):
        before = auth_metrics(self.operator)
        bad = self.wrong_signer.tenant()
        output = self.duckdb([f"TOKEN '{bad}'"], "SELECT 1;", succeeds=False)
        self.assertRegex(output, r"Permission Error: .*HTTP 401")
        self.assertNotIn(bad, output, "the token must not appear in the error")
        after = auth_metrics(self.operator)
        # a fixed token cannot be refreshed: exactly one refused request, no retry
        self.assertEqual(delta(before, after, ("enforce", "invalid", "duckdb")), 1)

    def test_b_fixed_token_reads_and_writes(self):
        before = auth_metrics(self.operator)
        self.round_trip([f"TOKEN '{self.signer.tenant()}'"], f"fixed_{RUN}")
        after = auth_metrics(self.operator)
        # (e) the server classified every request by User-Agent duckdb-hoglake/1
        self.assertGreater(delta(before, after, ("enforce", "ok", "duckdb")), 0)
        self.assertEqual(delta(before, after, ("enforce", "ok", "other")), 0)
        self.assertEqual(delta(before, after, ("enforce", "ok", "none")), 0)

    def test_c_client_credentials_mint_once_and_cache(self):
        self.endpoint.tokens = [
            (self.signer.tenant(), 600),
            (self.signer.tenant(), 600),
        ]
        self.round_trip(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                f"CLIENT_SECRET '{CLIENT_SECRET}'",
            ],
            f"cc_{RUN}",
        )
        # one mint served every request of the session
        self.assertEqual(len(self.endpoint.requests), 1, self.endpoint.requests)
        mint = self.endpoint.requests[0]
        self.assertEqual(mint["path"], "/oauth/token")
        self.assertEqual(mint["content_type"], "application/x-www-form-urlencoded")
        self.assertEqual(mint["user_agent"], "duckdb-hoglake/1")
        self.assertEqual(
            mint["form"], {"grant_type": "client_credentials", "audience": "hoglake"}
        )

    def test_c_audience_option_is_sent(self):
        self.endpoint.tokens = [(self.signer.tenant(), 600)]
        self.duckdb(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                f"CLIENT_SECRET '{CLIENT_SECRET}'",
                "AUDIENCE 'not hoglake'",
            ],
            "SELECT 1;",
            succeeds=False,
        )
        self.assertEqual(self.endpoint.requests[0]["form"]["audience"], "not hoglake")

    def test_c_wrong_client_secret_is_reported(self):
        output = self.duckdb(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                "CLIENT_SECRET 'wrong'",
            ],
            "SELECT 1;",
            succeeds=False,
        )
        self.assertIn("invalid_client", output)
        self.assertNotIn("wrong'", output.replace("CLIENT_SECRET 'wrong'", ""))

    def test_d_401_mints_again_and_retries_once(self):
        before = auth_metrics(self.operator)
        self.endpoint.tokens = [
            (self.wrong_signer.tenant(), 600),
            (self.signer.tenant(), 600),
        ]
        self.round_trip(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                f"CLIENT_SECRET '{CLIENT_SECRET}'",
            ],
            f"retry_{RUN}",
        )
        self.assertEqual(len(self.endpoint.requests), 2, self.endpoint.requests)
        after = auth_metrics(self.operator)
        self.assertEqual(delta(before, after, ("enforce", "invalid", "duckdb")), 1)

    def test_d_second_401_is_final(self):
        self.endpoint.tokens = [(self.wrong_signer.tenant(), 600)] * 3
        output = self.duckdb(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                f"CLIENT_SECRET '{CLIENT_SECRET}'",
            ],
            "SELECT 1;",
            succeeds=False,
        )
        self.assertRegex(output, r"Permission Error: .*HTTP 401")
        self.assertEqual(len(self.endpoint.requests), 2, self.endpoint.requests)

    def test_e_other_catalog_token_is_not_entitled(self):
        output = self.duckdb(
            [f"TOKEN '{self.signer.tenant(catalog='someone-else')}'"],
            "SELECT 1;",
            succeeds=False,
        )
        # hoglake answers 404 for a catalog the token does not name, so its
        # existence is not disclosed
        self.assertIn("Catalog Error", output)

    def test_e_read_scope_cannot_write(self):
        output = self.duckdb(
            [f"TOKEN '{self.signer.tenant(scopes=('read',))}'"],
            f"CREATE SCHEMA lake.readonly_{RUN};",
            succeeds=False,
        )
        self.assertRegex(output, r"Permission Error: .*HTTP 403")

    def test_secrets_are_not_kept_in_attach_options(self):
        token = self.signer.tenant()
        output = self.duckdb(
            [f"TOKEN '{token}'"],
            "SELECT 'OPTS|' || options::VARCHAR FROM duckdb_databases() WHERE database_name = 'lake';",
        )
        self.assertNotIn(token, output)
        self.assertIn("token=<redacted>", output)
        self.endpoint.tokens = [(self.signer.tenant(), 600)]
        output = self.duckdb(
            [
                f"TOKEN_URL '{self.endpoint.url}'",
                f"CLIENT_ID '{CLIENT_ID}'",
                f"CLIENT_SECRET '{CLIENT_SECRET}'",
            ],
            "SELECT 'OPTS|' || options::VARCHAR FROM duckdb_databases() WHERE database_name = 'lake';",
        )
        self.assertNotIn(CLIENT_SECRET, output)
        self.assertIn("client_secret=<redacted>", output)

    def test_f_author_is_the_token_subject(self):
        schema = f"author_{RUN}"
        self.round_trip([f"TOKEN '{self.signer.tenant()}'"], schema)
        authors = self.authors(schema)
        ddl = {author for author, message in authors if message.startswith("CREATE")}
        self.assertEqual(ddl, {SUBJECT}, authors)
        # The INSERT's commit carries no author from the extension. hoglake's
        # commit routes do not (yet) fill it from the token, so it lands NULL;
        # what must never appear is the unauthenticated claim.
        self.assertNotIn("duckdb-client", {author for author, _ in authors}, authors)


@unittest.skipUnless(MODE == "warn", "server not in warn mode")
class Warn(AuthBase):
    def test_no_token_succeeds_and_is_counted(self):
        before = auth_metrics(self.operator)
        schema = f"warn_{RUN}"
        self.round_trip([], schema)
        after = auth_metrics(self.operator)
        self.assertGreater(delta(before, after, ("warn", "missing", "duckdb")), 0)
        # unauthenticated, the extension's own claim is still the attribution
        authors = {
            author
            for author, message in self.authors(schema)
            if message.startswith("CREATE")
        }
        self.assertEqual(authors, {"duckdb-client"})

    def test_token_attribution_under_warn(self):
        schema = f"warntok_{RUN}"
        self.round_trip([f"TOKEN '{self.signer.tenant()}'"], schema)
        authors = {
            author
            for author, message in self.authors(schema)
            if message.startswith("CREATE")
        }
        self.assertEqual(authors, {SUBJECT})


if __name__ == "__main__":
    if MODE not in ("enforce", "warn"):
        sys.exit(
            "DUCKEXT_AUTH_MODE must be enforce or warn (the server's HOGLAKE_AUTH_MODE)"
        )
    unittest.main(verbosity=2)
