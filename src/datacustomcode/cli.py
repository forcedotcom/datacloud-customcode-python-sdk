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
from importlib import metadata
import json
import os
import sys
from typing import List, Union

import click
from loguru import logger

from datacustomcode import AuthType
from datacustomcode.auth import configure_oauth_tokens
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
        client_secret = click.prompt("Client Secret", hide_input=True)
        redirect_uri = click.prompt("Redirect URI")
        configure_oauth_tokens(
            login_url, client_id, client_secret, redirect_uri, profile
        )
    elif auth_type == AuthType.CLIENT_CREDENTIALS.value:
        _configure_client_credentials(login_url, client_id, profile)


@cli.command()
@click.option("--profile", default="default", help="Credential profile name")
def auth(profile: str):
    from datacustomcode.credentials import Credentials

    credentials = Credentials.from_available(profile=profile)
    if not credentials.redirect_uri:
        click.secho(
            "Error: Redirect URI is required for OAuth Tokens authentication",
            fg="red",
        )
        raise click.Abort()
    if credentials.auth_type == AuthType.OAUTH_TOKENS:
        configure_oauth_tokens(
            login_url=credentials.login_url,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            redirect_uri=credentials.redirect_uri,
            profile=profile,
        )


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
