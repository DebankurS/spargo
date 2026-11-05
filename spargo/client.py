"""Minimal Snowflake SQL API v2 client (stdlib only, plus `cryptography` for key-pair auth).

Works against real Snowflake and against the bundled SPCS emulator.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Callable


class SnowflakeError(RuntimeError):
    def __init__(self, message: str, code: str | None = None, sql_state: str | None = None):
        super().__init__(message)
        self.code = code
        self.sql_state = sql_state


# ---------------------------------------------------------------- auth

@dataclass
class Auth:
    """Produces (token, token_type) on demand so short-lived tokens can refresh."""
    get: Callable[[], tuple[str, str]]


def static_token_auth(token: str, token_type: str = "PROGRAMMATIC_ACCESS_TOKEN") -> Auth:
    return Auth(lambda: (token, token_type))


SPCS_TOKEN_FILE = "/snowflake/session/token"


def spcs_oauth_auth(path: str = SPCS_TOKEN_FILE) -> Auth:
    """Inside an SPCS container Snowflake mounts a rotating OAuth token; re-read every call."""
    def get():
        with open(path) as f:
            return f.read().strip(), "OAUTH"
    return Auth(get)


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def keypair_auth(account: str, user: str, private_key_pem: bytes, passphrase: bytes | None = None,
                 lifetime: int = 3000) -> Auth:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    key = serialization.load_pem_private_key(private_key_pem, password=passphrase)
    pub = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    fp = "SHA256:" + base64.b64encode(hashlib.sha256(pub).digest()).decode()
    acct = account.upper().split(".")[0]
    qualified = f"{acct}.{user.upper()}"
    cache: dict = {"exp": 0, "tok": ""}

    def get():
        now = int(time.time())
        if cache["exp"] - now < 60:
            hdr = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
            body = _b64url(json.dumps({"iss": f"{qualified}.{fp}", "sub": qualified,
                                       "iat": now, "exp": now + lifetime}).encode())
            sig = key.sign(f"{hdr}.{body}".encode(), padding.PKCS1v15(), hashes.SHA256())
            cache.update(tok=f"{hdr}.{body}.{_b64url(sig)}", exp=now + lifetime)
        return cache["tok"], "KEYPAIR_JWT"
    return Auth(get)


# ---------------------------------------------------------------- client

@dataclass
class Rows:
    columns: list[str]
    rows: list[dict]

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def first(self) -> dict | None:
        return self.rows[0] if self.rows else None


class SnowflakeClient:
    def __init__(self, base_url: str, auth: Auth, role: str | None = None,
                 warehouse: str | None = None, database: str | None = None,
                 schema: str | None = None, retries: int = 4, timeout: int = 90):
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.role, self.warehouse, self.database, self.schema = role, warehouse, database, schema
        self.retries, self.timeout = retries, timeout
        self.statements_run: list[str] = []  # audit trail, handy for tests / --verbose

    # -- http
    def _request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        token, ttype = self.auth.get()
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Snowflake-Authorization-Token-Type": ttype,
            "Content-Type": "application/json", "Accept": "application/json",
            "User-Agent": "spargo/0.1",
        }
        data = json.dumps(body).encode() if body is not None else None
        delay = 0.5
        for attempt in range(self.retries + 1):
            full = path + ("&retry=true" if attempt > 0 and "requestId=" in path else "")
            req = urllib.request.Request(self.base_url + full, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return r.status, json.loads(r.read() or b"{}")
            except urllib.error.HTTPError as e:
                payload = e.read()
                try:
                    parsed = json.loads(payload or b"{}")
                except ValueError:
                    parsed = {"message": payload.decode(errors="replace")}
                if e.code in (429, 502, 503, 504) and attempt < self.retries:
                    time.sleep(delay); delay *= 2
                    continue
                raise SnowflakeError(parsed.get("message") or f"HTTP {e.code}",
                                     parsed.get("code"), parsed.get("sqlState")) from None
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                if attempt < self.retries:
                    time.sleep(delay); delay *= 2
                    continue
                raise SnowflakeError(f"cannot reach Snowflake at {self.base_url}: {e}") from e
        raise AssertionError("unreachable")

    # -- sql
    def execute(self, sql: str, *, wait: float = 120.0) -> Rows:
        self.statements_run.append(sql)
        body = {"statement": sql, "timeout": 60}
        for k in ("role", "warehouse", "database", "schema"):
            if getattr(self, k):
                body[k] = getattr(self, k)
        handle = str(uuid.uuid4())
        status, resp = self._request("POST", f"/api/v2/statements?requestId={handle}&async=false", body)
        deadline = time.time() + wait
        while status == 202:
            if time.time() > deadline:
                raise SnowflakeError(f"statement timed out waiting for result: {sql[:80]}")
            time.sleep(0.5)
            status, resp = self._request("GET", resp["statementStatusUrl"])
        return self._to_rows(resp)

    def _to_rows(self, resp: dict) -> Rows:
        meta = resp.get("resultSetMetaData") or {}
        cols = [c["name"].upper() for c in meta.get("rowType", [])]
        data = list(resp.get("data") or [])
        parts = meta.get("partitionInfo") or []
        for n in range(1, len(parts)):
            _, more = self._request("GET", f"/api/v2/statements/{resp['statementHandle']}?partition={n}")
            data.extend(more.get("data") or [])
        return Rows(cols, [dict(zip(cols, r)) for r in data])


def from_env(env=os.environ) -> SnowflakeClient:
    """Build a client from SPARGO_* environment variables.

    SPARGO_ACCOUNT_URL    https://<account>.snowflakecomputing.com   (or emulator URL)
    SPARGO_AUTH           pat | oauth | keypair | spcs      (default: pat, or spcs when running in SPCS)
    SPARGO_TOKEN          for pat / oauth
    SPARGO_USER, SPARGO_ACCOUNT, SPARGO_PRIVATE_KEY_FILE, SPARGO_PRIVATE_KEY_PASSPHRASE  for keypair
    """
    in_spcs = os.path.exists(SPCS_TOKEN_FILE) and env.get("SNOWFLAKE_HOST")
    url = env.get("SPARGO_ACCOUNT_URL") or (f"https://{env['SNOWFLAKE_HOST']}" if in_spcs else None)
    if not url:
        raise SnowflakeError("SPARGO_ACCOUNT_URL is not set")
    mode = (env.get("SPARGO_AUTH") or ("spcs" if in_spcs else "pat")).lower()
    if mode == "spcs":
        auth = spcs_oauth_auth()
    elif mode in ("pat", "oauth"):
        tok = env.get("SPARGO_TOKEN")
        if not tok:
            raise SnowflakeError("SPARGO_TOKEN is not set")
        auth = static_token_auth(tok, "PROGRAMMATIC_ACCESS_TOKEN" if mode == "pat" else "OAUTH")
    elif mode == "keypair":
        try:
            with open(env["SPARGO_PRIVATE_KEY_FILE"], "rb") as f:
                pem = f.read()
            auth = keypair_auth(env["SPARGO_ACCOUNT"], env["SPARGO_USER"], pem,
                                (env.get("SPARGO_PRIVATE_KEY_PASSPHRASE") or "").encode() or None)
        except KeyError as e:
            raise SnowflakeError(f"keypair auth needs {e.args[0]} to be set") from None
    else:
        raise SnowflakeError(f"unknown SPARGO_AUTH {mode!r}")
    return SnowflakeClient(url, auth)
