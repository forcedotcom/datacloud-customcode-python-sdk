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
from __future__ import annotations

from html import unescape
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Union,
)

from loguru import logger
import pydantic
from pydantic import BaseModel
import requests

from datacustomcode.cmd import cmd_output
from datacustomcode.credentials import AuthType
from datacustomcode.scan import find_base_directory, get_package_type

if TYPE_CHECKING:
    from datacustomcode.credentials import Credentials

DATA_CUSTOM_CODE_PATH = "services/data/v63.0/ssot/data-custom-code"
DATA_TRANSFORMS_PATH = "services/data/v63.0/ssot/data-transforms"
AUTH_PATH = "services/oauth2/token"
WAIT_FOR_DEPLOYMENT_TIMEOUT = 3000

# Available compute types for Data Cloud deployments.
# Nomenclature used by COMPUTE_TYPES keys align with
# compute instances provisioned by Data Cloud.
COMPUTE_TYPES = {
    "CPU_L": "CPU_XS",  # Large CPU instance
    "CPU_XL": "CPU_S",  # X-Large CPU instance
    "CPU_2XL": "CPU_M",  # 2X-Large CPU instance (default)
    "CPU_4XL": "CPU_L",  # 4X-Large CPU instance
}


def _sanitize_api_name(name: str) -> str:
    """Sanitize an API name to comply with Salesforce naming rules.

    Replaces spaces and hyphens with underscores, removes invalid characters,
    collapses consecutive underscores, and strips leading/trailing underscores.
    """
    sanitized = re.sub(r"[ \-]", "_", name)
    sanitized = re.sub(r"[^\w]", "", sanitized)
    sanitized = re.sub(r"_+", "_", sanitized)
    sanitized = sanitized.strip("_")
    return sanitized


class CodeExtensionMetadata(BaseModel):
    name: str
    version: str
    description: str
    computeType: str
    codeType: str
    functionInvokeOptions: Union[list[str], None] = None

    def __init__(self, **data):
        name = data.get("name", "")
        sanitized = _sanitize_api_name(name)
        if sanitized != name:
            logger.warning(f"API name '{name}' was sanitized to '{sanitized}'")
            data["name"] = sanitized
        if not sanitized:
            raise ValueError(
                f"API name '{name}' is invalid and could not be sanitized to a"
                " valid name."
            )
        if not sanitized[0].isalpha():
            raise ValueError(
                f"API name '{sanitized}' must begin with a letter. "
                "The name can only contain underscores and alphanumeric"
                " characters, must begin with a letter, not include spaces,"
                " not end with an underscore, and not contain two consecutive"
                " underscores."
            )
        super().__init__(**data)


def _join_strip_url(*args: str) -> str:
    return "/".join(arg.strip("/") for arg in args)


JSONValue = Union[
    Dict[str, "JSONValue"], List["JSONValue"], str, int, float, bool, None
]


def _make_api_call(
    url: str,
    method: str,
    headers: Union[dict, None] = None,
    token: Union[str, None] = None,
    **kwargs,
) -> dict[str, JSONValue]:
    """Make a request to Data Cloud Custom Code API."""
    headers = headers or {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    logger.debug(f"Making API call: {method} {url}")
    logger.debug(f"Headers: {headers}")
    logger.debug(f"Request params: {kwargs}")

    response = requests.request(method=method, url=url, headers=headers, **kwargs)
    if response.status_code >= 400:
        logger.warning(f"Error Response Status: {response.status_code}")
        logger.debug(f"Error Response Headers: {response.headers}")
        logger.warning(f"Error Response Text: {response.text[:500]}")

    if not response.text or response.text.strip() == "":
        response.raise_for_status()
        raise ValueError(
            f"Received empty response from {method} {url}. "
            f"Status code: {response.status_code}"
        )

    try:
        json_response = response.json()
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response. Status: {response.status_code}")
        logger.error(f"Response text: {response.text[:500]}")
        raise ValueError(
            f"Invalid JSON response from {method} {url}. "
            f"Status code: {response.status_code}, "
            f"Response: {response.text[:200]}"
        ) from e

    response.raise_for_status()
    assert isinstance(
        json_response, dict
    ), f"Unexpected response type: {type(json_response)}"
    return json_response


class AccessTokenResponse(BaseModel):
    access_token: str
    instance_url: str


def _retrieve_access_token(credentials: Credentials) -> AccessTokenResponse:
    """Get an access token for the Salesforce API."""
    logger.debug("Getting oauth token...")

    url = f"{credentials.login_url.rstrip('/')}/{AUTH_PATH.lstrip('/')}"

    if credentials.auth_type == AuthType.OAUTH_TOKENS:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": credentials.refresh_token,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
        }
    elif credentials.auth_type == AuthType.CLIENT_CREDENTIALS:
        data = {
            "grant_type": "client_credentials",
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
        }
    else:
        raise ValueError(f"Unsupported auth_type: {credentials.auth_type}")

    response = _make_api_call(url, "POST", data=data)
    return AccessTokenResponse(**response)


def _retrieve_access_token_from_sf_cli(sf_cli_org: str) -> AccessTokenResponse:
    """Get an access token from the Salesforce CLI."""
    try:
        result = subprocess.run(
            ["sf", "org", "display", "--target-org", sf_cli_org, "--json"],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "The 'sf' command was not found. "
            "Please install Salesforce CLI: https://developer.salesforce.com/tools/salesforcecli"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"'sf org display' timed out for org '{sf_cli_org}'"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"'sf org display' failed for org '{sf_cli_org}'.\n"
            f"Ensure the org is authenticated via 'sf org login web'.\n"
            f"stderr: {exc.stderr.strip()}"
        ) from exc

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse 'sf org display' output: {exc}") from exc

    if data.get("status") != 0:
        raise RuntimeError(
            f"SF CLI error for org '{sf_cli_org}': "
            f"{data.get('message', 'unknown error')}"
        )

    org_result = data.get("result", {})
    access_token = org_result.get("accessToken")
    instance_url = org_result.get("instanceUrl")
    if not access_token or not instance_url:
        raise RuntimeError(
            f"'sf org display' did not return an access token or instance URL "
            f"for org '{sf_cli_org}'"
        )
    return AccessTokenResponse(access_token=access_token, instance_url=instance_url)


class CreateDeploymentResponse(BaseModel):
    fileUploadUrl: str


def create_deployment(
    access_token: AccessTokenResponse, metadata: CodeExtensionMetadata
) -> CreateDeploymentResponse:
    """Create a custom code deployment in the DataCloud."""
    url = _join_strip_url(access_token.instance_url, DATA_CUSTOM_CODE_PATH)
    body = dict[str, Any](
        {
            "label": metadata.name,
            "name": metadata.name,
            "description": metadata.description,
            "version": metadata.version,
            "computeType": metadata.computeType,
            "codeType": metadata.codeType,
        }
    )
    if metadata.functionInvokeOptions:
        body["functionInvokeOptions"] = metadata.functionInvokeOptions
    logger.debug(f"Creating deployment {metadata.name}...")
    try:
        response = _make_api_call(
            url, "POST", token=access_token.access_token, json=body
        )
        return CreateDeploymentResponse(**response)
    except requests.HTTPError as exc:
        if exc.response.status_code == 409:
            raise ValueError(
                f"Deployment {metadata.name} exists. Please use a different name."
            ) from exc
        raise


PLATFORM_ENV_VAR = "DOCKER_DEFAULT_PLATFORM=linux/amd64"
DOCKER_IMAGE_NAME = "datacloud-custom-code-dependency-builder"
DEPENDENCIES_ARCHIVE_NAME = "native_dependencies"
DEPENDENCIES_ARCHIVE_FULL_NAME = f"{DEPENDENCIES_ARCHIVE_NAME}.tar.gz"
DEPENDENCIES_ARCHIVE_PATH = os.path.join(
    "payload", "archives", DEPENDENCIES_ARCHIVE_FULL_NAME
)
PY_FILES_PATH = os.path.join("payload", "py-files")
ZIP_FILE_NAME = "deployment.zip"


def prepare_dependency_archive(directory: str, docker_network: str, package_type: str) -> None:
    cmd = f"docker images -q {DOCKER_IMAGE_NAME}"
    image_exists = cmd_output(cmd)

    if not image_exists:
        logger.info(f"Building docker image with docker network: {docker_network}...")
        cmd = docker_build_cmd(docker_network)
        cmd_output(cmd)

    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(
            f"Building dependencies with docker network: {docker_network}"
        )
        shutil.copy("requirements.txt", temp_dir)
        shutil.copy("build_native_dependencies.sh", temp_dir)
        cmd = docker_run_cmd(docker_network, temp_dir)
        cmd_output(cmd)

        if package_type == "function":
            source_py_files = os.path.join(temp_dir, "py-files")
            if not os.path.exists(source_py_files):
                raise FileNotFoundError(
                    f"Expected py-files directory not found at {source_py_files}. "
                    "Docker build may have failed."
                )
            os.makedirs(os.path.dirname(PY_FILES_PATH), exist_ok=True)
            if os.path.exists(PY_FILES_PATH):
                shutil.rmtree(PY_FILES_PATH)
            shutil.copytree(source_py_files, PY_FILES_PATH)
            logger.info(f"Dependencies copied to {PY_FILES_PATH}")
        else:
            archives_temp_path = os.path.join(
                temp_dir, DEPENDENCIES_ARCHIVE_FULL_NAME
            )
            os.makedirs(os.path.dirname(DEPENDENCIES_ARCHIVE_PATH), exist_ok=True)
            shutil.copy(archives_temp_path, DEPENDENCIES_ARCHIVE_PATH)
            logger.info(f"Dependencies archived to {DEPENDENCIES_ARCHIVE_PATH}")


def docker_build_cmd(network: str) -> str:
    cmd = (
        f"{PLATFORM_ENV_VAR} docker build -t {DOCKER_IMAGE_NAME} "
        f"--file Dockerfile.dependencies . "
    )

    if network != "default":
        cmd = cmd + f"--network {network}"
    logger.debug(f"Docker build command: {cmd}")
    return cmd


def docker_run_cmd(network: str, temp_dir) -> str:
    cmd = (
        f"{PLATFORM_ENV_VAR} docker run --rm "
        f"-v {temp_dir}:/workspace "
        f"{DOCKER_IMAGE_NAME} "
    )

    if network != "default":
        cmd = cmd + f"--network {network} "
    logger.debug(f"Docker run command: {cmd}")
    return cmd


class DeploymentsResponse(BaseModel):
    deploymentStatus: str


def get_deployments(
    access_token: AccessTokenResponse, metadata: CodeExtensionMetadata
) -> DeploymentsResponse:
    """Get all custom code deployments from the DataCloud."""
    url = _join_strip_url(
        access_token.instance_url, DATA_CUSTOM_CODE_PATH, metadata.name
    )
    response = _make_api_call(url, "GET", token=access_token.access_token)
    return DeploymentsResponse(**response)


def wait_for_deployment(
    access_token: AccessTokenResponse,
    metadata: CodeExtensionMetadata,
    callback: Union[Callable[[str], None], None] = None,
) -> None:
    """Wait for deployment to complete.

    Args:
        callback: Optional callback function that receives the deployment status
    """
    start_time = time.time()
    logger.info("Waiting for deployment to complete")

    while True:
        deployment_status = get_deployments(access_token, metadata)
        status = deployment_status.deploymentStatus
        if (time.time() - start_time) > WAIT_FOR_DEPLOYMENT_TIMEOUT:
            raise TimeoutError("Deployment timed out.")

        if callback:
            callback(status)
        if status == "Deployed":
            logger.info(
                f"Deployment completed.\nElapsed time: {time.time() - start_time}"
            )
            break
        time.sleep(1)


DATA_TRANSFORM_REQUEST_TEMPLATE: dict[str, Any] = {
    "nodes": {},
    "sources": {},
    "macros": {
        "macro.byoc": {
            "arguments": [{"name": "{SCRIPT_NAME}", "type": "BYOC_SCRIPT"}],
        }
    },
}


class BaseConfig(BaseModel):
    entryPoint: str


class DataTransformConfig(BaseConfig):
    sdkVersion: str
    dataspace: str
    permissions: Permissions


class FunctionConfig(BaseConfig):
    pass


class Permissions(BaseModel):
    read: Union[DloPermission]
    write: Union[DloPermission]


class DloPermission(BaseModel):
    dlo: list[str]


def get_config(directory: str) -> BaseConfig:
    """Get the code extension config from the config.json file."""
    config_path = os.path.join(directory, "config.json")
    try:
        with open(config_path, "r") as f:
            config = json.loads(f.read())
            base_directory = find_base_directory(config_path)
            package_type = get_package_type(base_directory)
        if package_type == "script":
            return DataTransformConfig(**config)
        elif package_type == "function":
            return FunctionConfig(**config)
        else:
            raise ValueError(f"Invalid package type: {package_type}")
    except FileNotFoundError as err:
        raise FileNotFoundError(f"config.json not found at {config_path}") from err
    except json.JSONDecodeError as err:
        raise ValueError(f"config.json at {config_path} is not valid JSON") from err
    except pydantic.ValidationError as err:
        missing_fields = [str(err["loc"][0]) for err in err.errors()]
        raise ValueError(
            f"config.json at {config_path} is missing required "
            f"fields: {', '.join(missing_fields)}"
        ) from err


def create_data_transform(
    directory: str,
    access_token: AccessTokenResponse,
    metadata: CodeExtensionMetadata,
    data_transform_config: DataTransformConfig,
) -> dict:
    """Create a data transform in the DataCloud."""
    script_name = metadata.name
    request_hydrated = DATA_TRANSFORM_REQUEST_TEMPLATE.copy()

    # Add nodes for each write DLO
    for i, dlo in enumerate(data_transform_config.permissions.write.dlo, 1):
        request_hydrated["nodes"][f"node{i}"] = {
            "relation_name": dlo,
            "config": {"materialized": "table"},
            "compiled_code": "",
        }

    # Add sources for each read DLO
    for i, dlo in enumerate(data_transform_config.permissions.read.dlo, 1):
        request_hydrated["sources"][f"source{i}"] = {"relation_name": dlo}

    request_hydrated["macros"]["macro.byoc"]["arguments"][0]["name"] = script_name

    body = {
        "definition": {
            "type": "DCSQL",
            "manifest": request_hydrated,
            "version": "56.0",
        },
        "label": f"{metadata.name}",
        "name": f"{metadata.name}",
        "type": "BATCH",
        "dataSpaceName": data_transform_config.dataspace,
    }

    url = _join_strip_url(access_token.instance_url, DATA_TRANSFORMS_PATH)
    response = _make_api_call(url, "POST", token=access_token.access_token, json=body)
    return response


def has_nonempty_requirements_file(directory: str) -> bool:
    """
    Check if requirements.txt exists in the given directory and has at least
    one non-comment line.
    Args:
        directory (str): The directory to check for requirements.txt.
    Returns:
        bool: True if requirements.txt exists and has a non-comment line,
        False otherwise.
    """
    # Look for requirements.txt in the parent directory of the given directory
    requirements_path = os.path.join(os.path.dirname(directory), "requirements.txt")

    try:
        if os.path.isfile(requirements_path):
            with open(requirements_path, "r", encoding="utf-8") as f:
                for line in f:
                    # Consider non-empty if any line is not a comment (ignoring
                    # leading whitespace)
                    if line.strip() and not line.lstrip().startswith("#"):
                        return True
    except Exception as e:
        logger.error(f"Error reading requirements.txt: {e}")
    return False


def upload_zip(file_upload_url: str) -> None:
    file_upload_url = unescape(file_upload_url)
    with open(ZIP_FILE_NAME, "rb") as zip_file:
        response = requests.put(
            file_upload_url, data=zip_file, headers={"Content-Type": "application/zip"}
        )
        response.raise_for_status()


def zip(
    directory: str,
    docker_network: str,
    package_type: str,
):
    # Create a zip file excluding .DS_Store files
    import zipfile

    # prepare payload only if requirements.txt is non-empty
    if has_nonempty_requirements_file(directory):
        prepare_dependency_archive(directory, docker_network, package_type)
    else:
        logger.info(
            f"Skipping dependency archive: requirements.txt is missing or empty "
            f"in {directory}"
        )

    logger.debug(f"Zipping directory... {directory}")

    with zipfile.ZipFile(ZIP_FILE_NAME, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(directory):
            # Skip .DS_Store files when adding to zip
            for file in files:
                if file != ".DS_Store":
                    abs_path = os.path.join(root, file)
                    arcname = os.path.relpath(abs_path, directory)
                    zipf.write(abs_path, arcname)

    logger.debug(f"Created zip file: {ZIP_FILE_NAME}")


def deploy_full(
    directory: str,
    metadata: CodeExtensionMetadata,
    credentials: Union["Credentials", AccessTokenResponse],
    docker_network: str,
    callback=None,
) -> AccessTokenResponse:
    """Deploy a data transform in the DataCloud."""
    if isinstance(credentials, AccessTokenResponse):
        access_token = credentials
    else:
        access_token = _retrieve_access_token(credentials)

    # prepare payload
    config = get_config(directory)

    # create deployment and upload payload
    deployment = create_deployment(access_token, metadata)
    zip(directory, docker_network, metadata.codeType)
    upload_zip(deployment.fileUploadUrl)
    wait_for_deployment(access_token, metadata, callback)

    # create data transform

    if isinstance(config, DataTransformConfig):
        create_data_transform(directory, access_token, metadata, config)
    return access_token


def run_data_transform(
    access_token: AccessTokenResponse, metadata: CodeExtensionMetadata
) -> dict:
    logger.debug(f"Triggering data transform {metadata.name}")
    url = _join_strip_url(
        access_token.instance_url, DATA_TRANSFORMS_PATH, metadata.name, "actions", "run"
    )
    return _make_api_call(url, "POST")
