"""HTTP front-end: Snowflake SQL API v2 (/api/v2/statements) plus /_emu/* admin endpoints."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import signal
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Account
from .runtime import DockerRuntime, FakeRuntime
from .sql import SqlError


def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class Auth:
    """Accepts a static token (PAT/OAuth) and/or a key-pair JWT signed by a registered public key."""

    def __init__(self, token: str | None = None, public_key_pem: bytes | None = None,
                 account: str = "EMU", user: str = "SPARGO"):
        self.token, self.pub, self.account, self.user = token, None, account.upper(), user.upper()
        if public_key_pem:
            from cryptography.hazmat.primitives import serialization
            self.pub = serialization.load_pem_public_key(public_key_pem)
            der = self.pub.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            self.fp = "SHA256:" + base64.b64encode(hashlib.sha256(der).digest()).decode()

    @property
    def open(self) -> bool:
        return self.token is None and self.pub is None

    def check(self, header: str | None, ttype: str | None) -> bool:
        if not header or not header.startswith("Bearer ") or not header[7:].strip():
            return False
        tok = header[7:].strip()
        if self.open:
            return True
        if self.token is not None and ttype in (None, "PROGRAMMATIC_ACCESS_TOKEN", "OAUTH") and tok == self.token:
            return True
        if self.pub is not None and ttype == "KEYPAIR_JWT":
            return self._jwt_ok(tok)
        return False

    def _jwt_ok(self, tok: str) -> bool:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        try:
            h, b, sig = tok.split(".")
            claims = json.loads(_b64d(b))
            self.pub.verify(_b64d(sig), f"{h}.{b}".encode(), padding.PKCS1v15(), hashes.SHA256())
        except (ValueError, InvalidSignature):
            return False
        q = f"{self.account}.{self.user}"
        return (claims.get("exp", 0) > time.time() and claims.get("sub") == q
                and claims.get("iss") == f"{q}.{self.fp}")


class Handler(BaseHTTPRequestHandler):
    account: Account
    auth: Auth
    results: dict = {}
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if getattr(self.server, "verbose", False):
            sys.stderr.write("[emu] " + fmt % args + "\n")

    def _send(self, code: int, body: dict):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def _authed(self) -> bool:
        if self.auth.check(self.headers.get("Authorization"),
                           self.headers.get("X-Snowflake-Authorization-Token-Type")):
            return True
        self._send(401, {"code": "390303", "message": "Invalid OAuth access token.", "sqlState": "08004"})
        return False

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/_emu/health":
            return self._send(200, {"ok": True})
        if not self._authed():
            return
        if u.path == "/_emu/state":
            return self._send(200, self._state())
        m = re.fullmatch(r"/api/v2/statements/([\w-]+)", u.path)
        if m and m.group(1) in self.results:
            part = int(parse_qs(u.query).get("partition", ["0"])[0])
            r = self.results[m.group(1)]
            return self._send(200, r if part == 0 else {"data": []})
        self._send(404, {"message": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if not self._authed():
            return
        try:
            body = self._body()
        except ValueError:
            return self._send(400, {"message": "invalid JSON"})
        if u.path == "/api/v2/statements":
            return self._statement(body)
        if re.fullmatch(r"/api/v2/statements/[\w-]+/cancel", u.path):
            return self._send(200, {"message": "canceled"})
        if u.path == "/_emu/load":        # {"service": "DB.SCH.NAME", "cpu": 90}  simulate CPU load for autoscaling
            with self.account.lock:
                s = self.account.services.get(body.get("service", "").upper())
                if not s:
                    return self._send(404, {"message": "no such service"})
                s.load = body.get("cpu")
            return self._send(200, {"ok": True})
        if u.path == "/_emu/kill":        # {"service":..., "instance": 0}   chaos: kill an instance's container
            with self.account.lock:
                s = self.account.services.get(body.get("service", "").upper())
                i = s.instances.get(int(body.get("instance", 0))) if s else None
                if not i or not i.handle:
                    return self._send(404, {"message": "no such running instance"})
                h = i.handle
            self.account.rt.stop(h) if self.account.rt.supports_network else self.account.rt.crash(h)
            return self._send(200, {"ok": True})
        self._send(404, {"message": "not found"})

    def _statement(self, body: dict):
        sql = body.get("statement")
        if not sql:
            return self._send(422, {"code": "000900", "message": "statement is required", "sqlState": "42000"})
        handle = str(uuid.uuid4())
        try:
            cols, rows = self.account.execute(sql, body.get("database"), body.get("schema"))
        except SqlError as e:
            return self._send(422, {"code": e.code, "message": str(e), "sqlState": e.state, "statementHandle": handle})
        resp = {
            "resultSetMetaData": {"numRows": len(rows), "format": "jsonv2",
                                  "rowType": [{"name": c, "type": "text", "nullable": True} for c in cols],
                                  "partitionInfo": [{"rowCount": len(rows), "uncompressedSize": 0}]},
            "data": rows, "code": "090001", "sqlState": "00000", "statementHandle": handle,
            "statementStatusUrl": f"/api/v2/statements/{handle}", "message": "Statement executed successfully.",
            "createdOn": int(time.time() * 1000),
        }
        self.results[handle] = resp
        if len(self.results) > 500:
            self.results.pop(next(iter(self.results)))
        self._send(200, resp)

    def _state(self) -> dict:
        a = self.account
        with a.lock:
            return {"pools": {n: a._pool_view(p) for n, p in a.pools.items()},
                    "services": {n: {**a._svc_row(s), "endpoints": {str(p): px.port for p, px in s.proxies.items()},
                                     "instances": [{"id": i.id, "status": i.status, "restarts": i.restarts,
                                                    "message": i.message, "ports": i.handle.ports if i.handle else {}}
                                                   for i in s.instances.values()]}
                                 for n, s in a.services.items()}}


def make_server(host: str = "127.0.0.1", port: int = 0, runtime=None, token: str | None = None,
                public_key_pem: bytes | None = None, start_delay: float = 0.0,
                tick: float = 0.3, verbose: bool = False):
    acct = Account(runtime or FakeRuntime(), start_delay=start_delay)
    handler = type("H", (Handler,), {"account": acct, "auth": Auth(token, public_key_pem), "results": {}})
    srv = ThreadingHTTPServer((host, port), handler)
    srv.daemon_threads = True
    srv.verbose = verbose
    srv.account = acct
    acct.start(tick)
    return srv


def main(argv=None):
    ap = argparse.ArgumentParser(prog="spcs-emulator", description="Local Snowpark Container Services emulator")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8085)
    ap.add_argument("--runtime", choices=["docker", "fake"], default="docker")
    ap.add_argument("--token", help="require this bearer token (PAT / OAuth style); default: accept any token")
    ap.add_argument("--jwt-public-key", help="PEM public key; enables key-pair JWT auth")
    ap.add_argument("--image-map", action="append", default=[], metavar="SPCS_IMAGE=DOCKER_IMAGE",
                    help="map an SPCS image path to a local/pullable docker image (repeatable)")
    ap.add_argument("--start-delay", type=float, default=1.0, help="seconds before a started instance is READY")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)

    imap = dict(x.split("=", 1) for x in a.image_map)
    rt = DockerRuntime(imap) if a.runtime == "docker" else FakeRuntime(imap)
    pem = open(a.jwt_public_key, "rb").read() if a.jwt_public_key else None
    srv = make_server(a.host, a.port, rt, a.token, pem, a.start_delay, verbose=a.verbose)
    print(f"SPCS emulator listening on http://{a.host}:{srv.server_address[1]} (runtime={a.runtime})", flush=True)

    def stop(*_):
        threading.Thread(target=srv.shutdown, daemon=True).start()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        srv.serve_forever()
    finally:
        srv.account.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
