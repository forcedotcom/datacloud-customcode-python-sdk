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
@click.option("--profile", default="default")
@click.option("--path", default="payload")
@click.option("--name", required=True)
@click.option("--version", default="0.0.1")
@click.option("--description", default="Custom Data Transform Code")
def deploy(profile: str, path: str, name: str, version: str, description: str):
    from datacustomcode.credentials import Credentials
    from datacustomcode.deploy import TransformationJobMetadata, deploy_full

    logger.debug("Deploying project")

    metadata = TransformationJobMetadata(
        name=name,
        version=version,
        description=description,
    )
    try:
        credentials = Credentials.from_ini(profile=profile)
    except KeyError:
        click.secho(
            f"Error: Profile {profile} not found in credentials.ini. "
            "Run `datacustomcode configure` to create a credentialsprofile.",
            fg="red",
        )
        raise click.Abort() from None
    deploy_full(path, metadata, credentials)


@cli.command()
@click.argument("directory", default=".")
def init(directory: str):
    from datacustomcode.template import copy_template

    click.echo("Copying template to " + click.style(directory, fg="blue", bold=True))
    copy_template(directory)
    click.echo(
        "Start developing by updating the code in "
        + click.style(f"{directory}/payload/entrypoint.py", fg="blue", bold=True)
    )


@cli.command()
@click.argument("filename")
@click.option("--config")
@click.option("--dry-run", is_flag=True)
def scan(filename: str, config: str, dry_run: bool):
    from datacustomcode.scan import dc_config_json_from_file

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


@cli.command()
@click.argument("entrypoint")
@click.option("--config-file", default=None)
@click.option("--dependencies", default=[], multiple=True)
def run(entrypoint: str, config_file: Union[str, None], dependencies: List[str]):
    from datacustomcode.run import run_entrypoint

    run_entrypoint(entrypoint, config_file, dependencies)
