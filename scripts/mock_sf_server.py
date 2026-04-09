"""Minimal mock Salesforce server for CI integration tests.

Intercepts the HTTP calls made during ``sf data-code-extension script|function run``
so that neither a real Salesforce org nor real Data Cloud data is required.

Endpoints handled
-----------------
GET  /services/oauth2/userinfo
    Called by ``sf org login access-token`` to resolve a username from a token,
    and by ``connection.refreshAuth()`` in the SF CLI plugin.

POST /services/oauth2/token
    Called by ``connection.refreshAuth()`` when it tries to exchange a token.

POST /services/data/v66.0/ssot/query-sql
    Called by SFCLIDataCloudReader when the script entrypoint reads a DLO/DMO.
    Returns fake rows with the columns expected by the default script template
    (Account_std__dll: description__c, sfdcorganizationid__c, kq_id__c).

GET  /* (catch-all)
    Returns {"status": "ok"} for any other SF API path the CLI may probe.

Usage
-----
    python scripts/mock_sf_server.py          # listens on port 8888
    MOCK_SF_PORT=9000 python scripts/mock_sf_server.py
    python scripts/mock_sf_server.py 9000
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import sys

PORT = (
    int(sys.argv[1])
    if len(sys.argv) > 1
    else int(os.environ.get("MOCK_SF_PORT", "8888"))
)

_USERINFO = {
    "sub": "https://test.salesforce.com/id/00D000000000001AAA/005000000000001AAA",
    "user_id": "005000000000001AAA",
    "organization_id": "00D000000000001AAA",
    "preferred_username": "dev1@example.com",
    "name": "Dev User",
    "email": "dev1@example.com",
    "username": "dev1@example.com",
    "active": True,
}

_TOKEN_RESPONSE = {
    "access_token": "fake_access_token_00D000000000001AAA",
    "instance_url": f"http://localhost:{PORT}",
    "token_type": "Bearer",
    "scope": "api",
}

# Fake rows for Account_std__dll (used by the default script template).
# Columns: description__c, sfdcorganizationid__c, kq_id__c
_QUERY_RESPONSE = {
    "metadata": [
        {"name": "description__c"},
        {"name": "sfdcorganizationid__c"},
        {"name": "kq_id__c"},
    ],
    "data": [
        ["hello world", "org123", "kq001"],
        ["another row", "org123", "kq002"],
    ],
}


class MockSFHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:  # type: ignore[override]
        print(f"[MOCK SF] {self.command} {self.path} — {fmt % args}", flush=True)

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/services/oauth2/userinfo":
            self._send_json(_USERINFO)
        else:
            self._send_json({"status": "ok"})

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        print(f"[MOCK SF] POST body: {body[:200]!r}", flush=True)

        if path == "/services/oauth2/token":
            self._send_json(_TOKEN_RESPONSE)
        elif path.endswith("/ssot/query-sql"):
            self._send_json(_QUERY_RESPONSE)
        else:
            self._send_json({"status": "ok"})


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), MockSFHandler)
    server.allow_reuse_address = True
    print(f"[MOCK SF] Listening on http://localhost:{PORT}", flush=True)
    server.serve_forever()
