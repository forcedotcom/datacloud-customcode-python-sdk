# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import http.server
import queue
import socketserver
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
import webbrowser

import click
import requests


class OAuthCallbackHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler to capture OAuth callback."""

    def __init__(self, *args, auth_code_queue=None, **kwargs):
        self.auth_code_queue = auth_code_queue
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET request from OAuth callback."""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        if "code" in query_params:
            auth_code = query_params["code"][0]
            self.auth_code_queue.put(auth_code)
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>Authentication successful!</h1>"
                b"<p>You can close this window and return to the terminal.</p>"
                b"</body></html>"
            )
        elif "error" in query_params:
            error = query_params["error"][0]
            error_description = query_params.get("error_description", [""])[0]
            self.auth_code_queue.put(f"ERROR:{error}:{error_description}")
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"<html><body><h1>Authentication failed</h1>"
                f"<p>Error: {error}</p>"
                f"<p>{error_description}</p></body></html>".encode()
            )
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Invalid callback</h1></body></html>")

    def log_message(self, format, *args):
        """Suppress default logging."""


def _run_oauth_callback_server(
    redirect_uri: str, auth_code_queue: "queue.Queue[str]"
) -> tuple[socketserver.TCPServer, int]:
    """Start a local HTTP server to catch OAuth callback.

    Args:
        redirect_uri: The redirect URI configured in the OAuth app
        auth_code_queue: Queue to put the authorization code in

    Returns:
        Tuple of (server instance, actual port number)
    """
    parsed_uri = urlparse(redirect_uri)
    host = parsed_uri.hostname
    port = parsed_uri.port
    if not host or not port:
        raise ValueError(f"Invalid redirect URI: {redirect_uri}")

    # Create a custom handler factory
    def handler_factory(*args, **kwargs):
        return OAuthCallbackHandler(*args, auth_code_queue=auth_code_queue, **kwargs)

    server = socketserver.TCPServer((host, port), handler_factory)
    server.allow_reuse_address = True

    def serve():
        server.serve_forever()

    server_thread = threading.Thread(target=serve, daemon=True)
    server_thread.start()

    # Wait a moment for server to start
    time.sleep(0.5)

    return server, port


def _exchange_code_for_tokens(
    login_url: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    auth_code: str,
) -> Any:
    """Exchange authorization code for access and refresh tokens.

    Args:
        login_url: Salesforce login URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        redirect_uri: Redirect URI used in authorization
        auth_code: Authorization code from callback

    Returns:
        Dictionary containing access_token and refresh_token

    Raises:
        click.ClickException: If token exchange fails
    """
    token_url = f"{login_url.rstrip('/')}/services/oauth2/token"
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }

    try:
        response = requests.post(token_url, data=data, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise click.ClickException(
            f"Failed to exchange authorization code for tokens: {e}"
        ) from e


def do_oauth_browser_flow(
    login_url: str, client_id: str, client_secret: str, redirect_uri: str
) -> tuple[str, str]:
    """Perform OAuth browser flow to obtain tokens.

    Args:
        login_url: Salesforce login URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        redirect_uri: Redirect URI configured in OAuth app

    Returns:
        Tuple of (refresh_token, access_token)

    Raises:
        click.ClickException: If OAuth flow fails
    """
    # Create queue for communication between server and main thread
    auth_code_queue: queue.Queue[str] = queue.Queue()

    # Start callback server
    click.echo(f"\nStarting local callback server on {redirect_uri}...")
    server, actual_port = _run_oauth_callback_server(redirect_uri, auth_code_queue)

    # Build authorization URL with final redirect_uri
    auth_url = (
        f"{login_url.rstrip('/')}/services/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
    )

    # Open browser
    click.echo("Opening browser for authentication...")
    click.echo(f"If the browser doesn't open automatically, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Wait for callback (with timeout)
    click.echo("Waiting for authentication...")
    try:
        result = auth_code_queue.get(timeout=60)  # 1 minute timeout
    except queue.Empty:
        server.shutdown()
        raise click.ClickException(
            "Authentication timeout. Please try again."
        ) from None

    # Shutdown server
    server.shutdown()

    # Check for errors
    if result.startswith("ERROR:"):
        _, error, error_description = result.split(":", 2)
        raise click.ClickException(f"OAuth error: {error}. {error_description}")

    auth_code = result

    # Exchange code for tokens
    click.echo("Exchanging authorization code for tokens...")
    token_response = _exchange_code_for_tokens(
        login_url, client_id, client_secret, redirect_uri, auth_code
    )

    refresh_token = token_response.get("refresh_token")
    access_token = token_response.get("access_token")

    if not refresh_token:
        raise click.ClickException(
            "No refresh_token in response. Please check your OAuth app configuration."
        )

    return refresh_token, access_token


def configure_oauth_tokens(
    login_url: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    profile: str,
) -> None:
    """Configure credentials for OAuth Tokens authentication."""
    from datacustomcode.credentials import AuthType, Credentials

    # Perform OAuth browser flow
    try:
        refresh_token, access_token = do_oauth_browser_flow(
            login_url, client_id, client_secret, redirect_uri
        )
    except click.ClickException as e:
        click.secho(f"Error: {e}", fg="red")
        raise click.Abort() from None

    credentials = Credentials(
        login_url=login_url,
        client_id=client_id,
        auth_type=AuthType.OAUTH_TOKENS,
        client_secret=client_secret,
        refresh_token=refresh_token,
        access_token=access_token,
        redirect_uri=redirect_uri,
    )
    credentials.update_ini(profile=profile)
    click.secho(
        f"OAuth Tokens credentials saved to profile '{profile}' successfully",
        fg="green",
    )
