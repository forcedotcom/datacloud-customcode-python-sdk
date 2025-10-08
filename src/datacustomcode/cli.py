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


@cli.command()
@click.option("--profile", default="default")
@click.option("--username", prompt=True)
@click.option("--password", prompt=True, hide_input=True)
@click.option("--client-id", prompt=True)
@click.option("--client-secret", prompt=True)
@click.option("--login-url", prompt=True)
def configure(
    username: str,
    password: str,
    client_id: str,
    client_secret: str,
    login_url: str,
    profile: str,
) -> None:
    from datacustomcode.credentials import Credentials

    Credentials(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
        login_url=login_url,
    ).update_ini(profile=profile)


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
def deploy(
    path: str,
    name: str,
    version: str,
    description: str,
    cpu_size: str,
    profile: str,
    network: str,
):
    from datacustomcode.credentials import Credentials
    from datacustomcode.deploy import TransformationJobMetadata, deploy_full

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

    metadata = TransformationJobMetadata(
        name=name,
        version=version,
        description=description,
        computeType=COMPUTE_TYPES[cpu_size],
    )
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
def init(directory: str):
    from datacustomcode.scan import dc_config_json_from_file
    from datacustomcode.template import copy_template

    click.echo("Copying template to " + click.style(directory, fg="blue", bold=True))
    copy_template(directory)
    entrypoint_path = os.path.join(directory, "payload", "entrypoint.py")
    config_location = os.path.join(os.path.dirname(entrypoint_path), "config.json")
    config_json = dc_config_json_from_file(entrypoint_path)
    with open(config_location, "w") as f:
        json.dump(config_json, f, indent=2)

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
    from datacustomcode.scan import dc_config_json_from_file, write_requirements_file

    config_location = config or os.path.join(os.path.dirname(filename), "config.json")
    click.echo(
        "Dumping scan results to config file: "
        + click.style(config_location, fg="blue", bold=True)
    )
    click.echo("Scanning " + click.style(filename, fg="blue", bold=True) + "...")
    config_json = dc_config_json_from_file(filename)

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
