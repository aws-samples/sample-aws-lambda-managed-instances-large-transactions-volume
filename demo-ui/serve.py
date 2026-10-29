#!/usr/bin/env python3
"""Local dev server for the LMI Ticketing Demo UI.

Two responsibilities:

1. Serves the static UI (index.html, app.js, config.js, styles.css, vendor/)
   with Cache-Control: no-store so edits show up on reload without
   hard-refresh gymnastics. Listens on 127.0.0.1 only — this is a
   developer tool, not a shared service, and must not accept connections
   from the network.

2. Acts as a SigV4-signing proxy for the simulation API. Both the
   simulation API and the ticketing API are deployed with
   AuthorizationType: AWS_IAM, so every request must be signed with the
   operator's AWS credentials. Rather than exposing those credentials to
   the browser, the UI calls same-origin paths like
     POST /proxy/simulation/start
     GET  /proxy/dashboard?run_id=...
   and this server signs them with botocore's SigV4Auth before forwarding
   to the real API Gateway endpoint. The browser never sees an AWS
   credential.

Configuration (environment variables):

  SIMULATION_API_URL   Required. Base URL of the deployed simulation API
                       (CloudFormation output SimulationApiEndpoint),
                       e.g. https://abc123.execute-api.us-east-1.amazonaws.com/v1

  AWS_REGION / AWS_DEFAULT_REGION
                       Target region for SigV4 signing. If unset the
                       server uses boto3's default resolution
                       (~/.aws/config, EC2 metadata, etc).

  AWS_PROFILE          Named profile to load credentials from.

Prerequisites: `pip install boto3` on your workstation. If you already
run AWS CLI v2, boto3 is probably available in the same environment.

Usage:
    export SIMULATION_API_URL="https://xxxxx.execute-api.us-east-1.amazonaws.com/v1"
    python3 serve.py           # listens on 127.0.0.1:8000
    python3 serve.py 9090      # listens on 127.0.0.1:9090
"""
from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Loopback bind: this is a developer-tool HTTP server with the operator's
# AWS credentials in process memory. It must never accept connections from
# anywhere other than the same machine. Changing BIND_HOST to 0.0.0.0 would
# expose signed-request forging to anyone on the local network.
BIND_HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

# Proxy configuration. SIMULATION_API_URL may be absent during local-only
# static-UI development; the server will still serve files but return 503
# on /proxy/* paths with a message telling the operator which env var to
# set.
_SIM_API_URL = os.environ.get("SIMULATION_API_URL", "").rstrip("/")

# Lazy-initialized SigV4 context. Avoiding a hard dependency on boto3 at
# import time means the static-file path still works even if boto3 is not
# installed; the proxy path returns 503 with an install hint.
_sigv4_ctx: dict | None = None


def _get_sigv4_ctx() -> tuple[dict | None, str]:
    """Return a cached {'auth': SigV4Auth, 'region': str} or (None, reason).

    Credentials are refreshed on every proxied request because botocore
    manages STS session rotation inside the credentials object; we hold a
    reference, not a frozen copy.
    """
    global _sigv4_ctx
    if _sigv4_ctx is not None:
        return _sigv4_ctx, ""
    try:
        import boto3  # noqa: PLC0415 - lazy import by design
        from botocore.auth import SigV4Auth  # noqa: PLC0415
    except ImportError:
        return None, "boto3 is not installed; run: pip install boto3"
    session = boto3.Session()
    credentials = session.get_credentials()
    if credentials is None:
        return None, "no AWS credentials found (set AWS_PROFILE or configure ~/.aws/credentials)"
    region = session.region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    _sigv4_ctx = {
        "auth": SigV4Auth(credentials, "execute-api", region),
        "region": region,
    }
    return _sigv4_ctx, ""


def _sign_and_forward(method: str, path_with_query: str, body: bytes) -> tuple[int, bytes, dict]:
    """SigV4-sign and forward a request to the simulation API.

    path_with_query is the substring that follows /proxy in the incoming
    URL — for example "/simulation/start" or "/dashboard?run_id=xyz". It
    is appended verbatim to SIMULATION_API_URL.
    Returns (status, body, headers) suitable for echoing back to the
    browser. Headers from the upstream are filtered to a safe subset.
    """
    if not _SIM_API_URL:
        return 503, b'{"error":"proxy_unconfigured","detail":"SIMULATION_API_URL env var is not set"}', {"Content-Type": "application/json"}

    ctx, reason = _get_sigv4_ctx()
    if ctx is None:
        return 503, json.dumps({"error": "sigv4_unavailable", "detail": reason}).encode(), {"Content-Type": "application/json"}

    from botocore.awsrequest import AWSRequest  # noqa: PLC0415 - lazy import by design

    target_url = f"{_SIM_API_URL}{path_with_query}"
    headers = {"Content-Type": "application/json"}
    aws_req = AWSRequest(method=method, url=target_url, data=body if body else b"", headers=headers)
    ctx["auth"].add_auth(aws_req)
    # Convert AWSRequest to a urllib.request.Request. botocore sets the
    # Host / Authorization / X-Amz-* headers on aws_req.headers after
    # add_auth(); we flow those through verbatim.
    req = urllib.request.Request(aws_req.url, method=aws_req.method, data=aws_req.body or None)
    for header_name, header_value in aws_req.headers.items():
        req.add_header(header_name, header_value)

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 - target is operator-controlled
            resp_body = resp.read()
            resp_headers = {"Content-Type": resp.headers.get("Content-Type", "application/json")}
            return resp.status, resp_body, resp_headers
    except urllib.error.HTTPError as exc:
        resp_body = exc.read() if hasattr(exc, "read") else str(exc).encode()
        return exc.code, resp_body, {"Content-Type": exc.headers.get("Content-Type", "application/json") if exc.headers else "application/json"}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Proxy forwarding failed")
        return 502, json.dumps({"error": "bad_gateway", "detail": str(exc)}).encode(), {"Content-Type": "application/json"}


class ProxyingNoCacheHandler(SimpleHTTPRequestHandler):
    """Serves static files with no-cache headers; proxies /proxy/* with SigV4."""

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _handle_proxy(self, method: str) -> None:
        # Same-origin / anti-rebinding checks before any signing happens.
        # The 127.0.0.1 bind keeps other machines out, but the operator's
        # own browser remains a path by which a page in another tab can
        # reach this proxy. Three complementary checks close that path:
        #   1. Host header must be a loopback form that includes the port
        #      the server is bound to. Blocks DNS-rebinding attempts where
        #      an attacker-controlled hostname resolves to 127.0.0.1 and
        #      the browser forwards the attacker's Host header.
        #   2. If Origin is present it must match the loopback origin.
        #      Blocks cross-origin fetches that include an Origin header
        #      (modern browsers set it on cross-origin POST). Origin is
        #      only required when present so that curl / debugging from
        #      the operator's terminal (no Origin header) still works.
        #   3. POST requires Content-Type: application/json. This moves
        #      cross-origin POSTs out of the CORS "simple request" category
        #      (text/plain, form-urlencoded, multipart) and into the
        #      preflight-required category. The server does not implement
        #      OPTIONS, so preflights return 501 and browsers never send
        #      the real request. app.js already sends application/json
        #      explicitly, so legitimate UI calls are unaffected.
        # See docs/threat-model.md E-5 for the attack this blocks.
        port = self.server.server_address[1]
        allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
        allowed_origins = {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}

        host = (self.headers.get("Host") or "").strip().lower()
        if host not in allowed_hosts:
            logger.warning("Proxy rejected: Host=%r not in %s", host, sorted(allowed_hosts))
            self.send_error(403, "Forbidden: Host must be loopback")
            return

        origin = self.headers.get("Origin")
        if origin is not None and origin.strip().lower() not in allowed_origins:
            logger.warning("Proxy rejected: Origin=%r not in %s", origin, sorted(allowed_origins))
            self.send_error(403, "Forbidden: cross-origin request")
            return

        if method == "POST":
            ctype = (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
            if ctype != "application/json":
                logger.warning("Proxy rejected: Content-Type=%r (require application/json on POST)", ctype)
                self.send_error(415, "Unsupported Media Type: application/json required on POST")
                return

        # Strip the leading /proxy prefix; the rest is appended to the
        # upstream base URL.
        parsed = urllib.parse.urlsplit(self.path)
        rest = parsed.path[len("/proxy"):]
        if parsed.query:
            rest = f"{rest}?{parsed.query}"
        body = b""
        if method in ("POST", "PUT", "PATCH"):
            length = int(self.headers.get("Content-Length") or "0")
            if length:
                body = self.rfile.read(length)
        status, resp_body, resp_headers = _sign_and_forward(method, rest, body)
        self.send_response(status)
        for header_name, header_value in resp_headers.items():
            self.send_header(header_name, header_value)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        self.wfile.write(resp_body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib signature
        if self.path.startswith("/proxy/"):
            self._handle_proxy("GET")
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802 - stdlib signature
        if self.path.startswith("/proxy/"):
            self._handle_proxy("POST")
            return
        self.send_error(405, "Method Not Allowed")

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        logger.info("%s - %s", self.address_string(), format % args)


if __name__ == "__main__":
    with ThreadingHTTPServer((BIND_HOST, PORT), ProxyingNoCacheHandler) as httpd:
        print(f"Serving on http://{BIND_HOST}:{PORT}/ (no-cache)")
        if _SIM_API_URL:
            print(f"Proxying /proxy/* to {_SIM_API_URL} with SigV4 signatures")
        else:
            print("WARN: SIMULATION_API_URL is not set; /proxy/* paths will return 503")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
