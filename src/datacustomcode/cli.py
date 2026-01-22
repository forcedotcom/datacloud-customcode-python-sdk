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
from importlib import metadata
import json
import os
import queue
import socketserver
import sys
import threading
import time
from typing import (
    Any,
    List,
    Union,
)
from urllib.parse import parse_qs, urlparse
import webbrowser

import click
from loguru import logger
import requests

from datacustomcode.scan import find_base_directory, get_package_type


@click.group()
@click.option("--debug", is_flag=True)
def cli(debug: bool):
    logger.remove()
    if debug:
        logger.configure(handlers=[{"sink": sys.stderr, "level": "DEBUG"}])
    else:
        logger.configure(handlers=[{"sink": sys.stderr, "level": "INFO"}])


@cli.command()
def version():
    """Display the current version of the package."""
    print(__name__)
    try:
        version = metadata.version("salesforce-data-customcode")
        click.echo(f"salesforce-data-customcode version: {version}")
    except metadata.PackageNotFoundError:
        click.echo("Version information not available")


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
    host = parsed_uri.hostname or "localhost"
    port = parsed_uri.port or 5555

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


def _perform_oauth_browser_flow(
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
    # Parse redirect_uri and ensure it has a port
    parsed_redirect = urlparse(redirect_uri)
    if not parsed_redirect.port:
        # If no port specified, default to 5555 and update redirect_uri
        default_port = 5555
        redirect_uri = f"{parsed_redirect.scheme}://{parsed_redirect.hostname}:{default_port}{parsed_redirect.path}"

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


def _configure_oauth_tokens(
    login_url: str,
    client_id: str,
    profile: str,
) -> None:
    """Configure credentials for OAuth Tokens authentication."""
    from datacustomcode.credentials import AuthType, Credentials

    client_secret = click.prompt("Client Secret", hide_input=True)
    redirect_uri = click.prompt("Redirect URI")

    # Perform OAuth browser flow
    try:
        refresh_token, access_token = _perform_oauth_browser_flow(
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
        core_token=access_token,
    )
    credentials.update_ini(profile=profile)
    click.secho(
        f"OAuth Tokens credentials saved to profile '{profile}' successfully",
        fg="green",
    )


def _configure_client_credentials(
    login_url: str,
    client_id: str,
    profile: str,
) -> None:
    """Configure credentials for Client Credentials authentication."""
    from datacustomcode.credentials import AuthType, Credentials

    client_secret = click.prompt("Client Secret")

    credentials = Credentials(
        login_url=login_url,
        client_id=client_id,
        auth_type=AuthType.CLIENT_CREDENTIALS,
        client_secret=client_secret,
    )
    credentials.update_ini(profile=profile)
    click.secho(
        f"Client Credentials saved to profile '{profile}' successfully",
        fg="green",
    )


@cli.command()
@click.option("--profile", default="default", help="Credential profile name")
@click.option(
    "--auth-type",
    type=click.Choice(["oauth_tokens", "client_credentials"]),
    default="oauth_tokens",
    help="""Authentication method to use.

    \b
    oauth_tokens       - OAuth tokens (refresh_token) authentication (default)
    client_credentials - Server-to-server using client_id/secret only
    """,
)
def configure(profile: str, auth_type: str) -> None:
    """Configure credentials for connecting to Data Cloud."""
    from datacustomcode.credentials import AuthType

    # Common fields for all auth types
    click.echo(f"\nConfiguring {auth_type} authentication for profile '{profile}':\n")
    login_url = click.prompt("Login URL")
    client_id = click.prompt("Client ID")

    # Route to appropriate handler based on auth type
    if auth_type == AuthType.OAUTH_TOKENS.value:
        _configure_oauth_tokens(login_url, client_id, profile)
    elif auth_type == AuthType.CLIENT_CREDENTIALS.value:
        _configure_client_credentials(login_url, client_id, profile)


@cli.command()
@click.argument("path", default="payload")
@click.option("--network", default="default")
def zip(path: str, network: str):
    from datacustomcode.deploy import zip

    logger.debug("Zipping project")
    zip(path, network)


@cli.command()
@click.option("--path", default="payload")
@click.option("--name", required=True)
@click.option("--version", default="0.0.1")
@click.option("--description", default="Custom Data Transform Code")
@click.option("--profile", default="default")
@click.option("--network", default="default")
@click.option(
    "--cpu-size",
    default="CPU_2XL",
    help="""CPU size for deployment. Available options:

    \b
    CPU_L     - Large CPU instance
    CPU_XL    - X-Large CPU instance
    CPU_2XL   - 2X-Large CPU instance [DEFAULT]
    CPU_4XL   - 4X-Large CPU instance

    Choose based on your workload requirements.""",
)
@click.option("--function-invoke-opt")
def deploy(
    path: str,
    name: str,
    version: str,
    description: str,
    cpu_size: str,
    profile: str,
    network: str,
    function_invoke_opt: str,
):
    from datacustomcode.credentials import Credentials
    from datacustomcode.deploy import CodeExtensionMetadata, deploy_full

    logger.debug("Deploying project")

    # Validate compute type
    from datacustomcode.deploy import COMPUTE_TYPES

    if cpu_size not in COMPUTE_TYPES.keys():
        click.secho(
            f"Error: Invalid CPU size '{cpu_size}'. "
            f"Available options: {', '.join(COMPUTE_TYPES.keys())}",
            fg="red",
        )
        raise click.Abort()

    logger.debug(f"Deploying with CPU size: {cpu_size}")
    base_directory = find_base_directory(path)
    package_type = get_package_type(base_directory)
    metadata = CodeExtensionMetadata(
        name=name,
        version=version,
        description=description,
        computeType=COMPUTE_TYPES[cpu_size],
        codeType=package_type,
    )

    if package_type == "function":
        if not function_invoke_opt:
            click.secho(
                "Error: Function invoke options are required for function package type",
                fg="red",
            )
            raise click.Abort()
        else:
            function_invoke_options = function_invoke_opt.split(",")
            metadata.functionInvokeOptions = function_invoke_options

    try:
        credentials = Credentials.from_available(profile=profile)
    except ValueError as e:
        click.secho(
            f"Error: {e}",
            fg="red",
        )
        raise click.Abort() from None
    deploy_full(path, metadata, credentials, network)


@cli.command()
@click.argument("directory", default=".")
@click.option(
    "--code-type", default="script", type=click.Choice(["script", "function"])
)
def init(directory: str, code_type: str):
    from datacustomcode.scan import (
        dc_config_json_from_file,
        update_config,
        write_sdk_config,
    )
    from datacustomcode.template import copy_function_template, copy_script_template

    click.echo("Copying template to " + click.style(directory, fg="blue", bold=True))
    if code_type == "script":
        copy_script_template(directory)
    elif code_type == "function":
        copy_function_template(directory)
    entrypoint_path = os.path.join(directory, "payload", "entrypoint.py")
    config_location = os.path.join(os.path.dirname(entrypoint_path), "config.json")

    # Write package type to SDK-specific config
    sdk_config = {"type": code_type}
    write_sdk_config(directory, sdk_config)

    config_json = dc_config_json_from_file(entrypoint_path, code_type)
    with open(config_location, "w") as f:
        json.dump(config_json, f, indent=2)

    updated_config_json = update_config(entrypoint_path)
    with open(config_location, "w") as f:
        json.dump(updated_config_json, f, indent=2)
    click.echo(
        "Start developing by updating the code in "
        + click.style(entrypoint_path, fg="blue", bold=True)
    )
    click.echo(
        "You can run "
        + click.style(f"datacustomcode scan {entrypoint_path}", fg="blue", bold=True)
        + " to automatically update config.json when you make changes to your code"
    )


@cli.command()
@click.argument("filename")
@click.option("--config")
@click.option("--dry-run", is_flag=True)
@click.option(
    "--no-requirements", is_flag=True, help="Skip generating requirements.txt file"
)
def scan(filename: str, config: str, dry_run: bool, no_requirements: bool):
    from datacustomcode.scan import update_config, write_requirements_file

    config_location = config or os.path.join(os.path.dirname(filename), "config.json")
    click.echo(
        "Dumping scan results to config file: "
        + click.style(config_location, fg="blue", bold=True)
    )
    click.echo("Scanning " + click.style(filename, fg="blue", bold=True) + "...")
    config_json = update_config(filename)

    click.secho(json.dumps(config_json, indent=2), fg="yellow")
    if not dry_run:
        with open(config_location, "w") as f:
            json.dump(config_json, f, indent=2)

        if not no_requirements:
            requirements_path = write_requirements_file(filename)
            click.echo(
                "Generated requirements file: "
                + click.style(requirements_path, fg="blue", bold=True)
            )


@cli.command()
@click.argument("entrypoint")
@click.option("--config-file", default=None)
@click.option("--dependencies", default=[], multiple=True)
@click.option("--profile", default="default")
def run(
    entrypoint: str,
    config_file: Union[str, None],
    dependencies: List[str],
    profile: str,
):
    from datacustomcode.run import run_entrypoint

    run_entrypoint(entrypoint, config_file, dependencies, profile)
