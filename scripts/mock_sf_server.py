"""Minimal mock Salesforce server for CI integration tests.

Intercepts the HTTP calls made during ``sf data-code-extension script|function``
``run`` and ``deploy`` so that neither a real Salesforce org nor real Data Cloud
data is required.

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

POST /services/data/v63.0/ssot/data-custom-code
    Called by deploy_full() → create_deployment().
    Returns a fake fileUploadUrl pointing back at this server.

GET  /services/data/v63.0/ssot/data-custom-code/*
    Called by deploy_full() → wait_for_deployment() → get_deployments().
    Returns deploymentStatus=Deployed immediately so the poll loop exits.

PUT  /upload/*
    The presigned fileUploadUrl target.  Accepts the deployment.zip binary.

POST /services/data/v63.0/ssot/data-transforms
    Called by deploy_full() → create_data_transform() for script packages.

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
    "access_token": "00D000000000001AAA!fakeAccessTokenForCITesting",
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

_DATA_CUSTOM_CODE_PATH = "/services/data/v63.0/ssot/data-custom-code"
_DATA_TRANSFORMS_PATH = "/services/data/v63.0/ssot/data-transforms"


class MockSFHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_empty(self, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _drain_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def log_message(self, fmt: str, *args: object) -> None:  # type: ignore[override]
        print(f"[MOCK SF] {self.command} {self.path} — {fmt % args}", flush=True)

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/services/oauth2/userinfo":
            self._send_json(_USERINFO)
        elif path.startswith(_DATA_CUSTOM_CODE_PATH + "/"):
            # Deployment status poll — report Deployed immediately
            self._send_json({"deploymentStatus": "Deployed"})
        else:
            self._send_json({"status": "ok"})

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        body = self._drain_body()
        print(f"[MOCK SF] POST body: {body[:300]!r}", flush=True)

        if path == "/services/oauth2/token":
            self._send_json(_TOKEN_RESPONSE)
        elif path.endswith("/ssot/query-sql"):
            # Data Cloud query (run command)
            self._send_json(_QUERY_RESPONSE)
        elif path == _DATA_CUSTOM_CODE_PATH:
            # create_deployment() — return a presigned upload URL
            self._send_json(
                {"fileUploadUrl": f"http://localhost:{PORT}/upload/fake-deployment.zip"}
            )
        elif path == _DATA_TRANSFORMS_PATH:
            # create_data_transform() — script packages only
            self._send_json({"id": "fake-dt-id", "name": "fake-data-transform"})
        else:
            self._send_json({"status": "ok"})

    def do_PUT(self) -> None:
        # upload_zip() sends the deployment.zip to the presigned URL
        self._drain_body()
        self._send_empty(200)


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), MockSFHandler)
    server.allow_reuse_address = True
    print(f"[MOCK SF] Listening on http://localhost:{PORT}", flush=True)
    server.serve_forever()
