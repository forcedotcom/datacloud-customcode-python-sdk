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
import shutil
import tarfile
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
from pydantic import BaseModel
import requests

from datacustomcode.cmd import cmd_output
from datacustomcode.scan import scan_file

if TYPE_CHECKING:
    from datacustomcode.credentials import Credentials

DATA_CUSTOM_CODE_PATH = "services/data/v63.0/ssot/data-custom-code"
DATA_TRANSFORMS_PATH = "services/data/v63.0/ssot/data-transforms"
AUTH_PATH = "services/oauth2/token"
WAIT_FOR_DEPLOYMENT_TIMEOUT = 3000


class TransformationJobMetadata(BaseModel):
    name: str
    version: str
    description: str


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
    response.raise_for_status()
    json_response = response.json()
    assert isinstance(
        json_response, dict
    ), f"Unexpected response type: {type(json_response)}"
    return json_response


class AccessTokenResponse(BaseModel):
    access_token: str
    instance_url: str


def _retrieve_access_token(credentials: Credentials) -> AccessTokenResponse:
    """Get a token for the Salesforce API."""
    logger.debug("Getting oauth token...")

    url = f"{credentials.login_url.rstrip('/')}/{AUTH_PATH.lstrip('/')}"

    data = {
        "grant_type": "password",
        "username": credentials.username,
        "password": credentials.password,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
    }
    response = _make_api_call(url, "POST", data=data)
    return AccessTokenResponse(**response)


class CreateDeploymentResponse(BaseModel):
    fileUploadUrl: str


def create_deployment(
    access_token: AccessTokenResponse, metadata: TransformationJobMetadata
) -> CreateDeploymentResponse:
    """Create a custom code deployment in the DataCloud."""
    url = _join_strip_url(access_token.instance_url, DATA_CUSTOM_CODE_PATH)
    body = {
        "label": metadata.name,
        "name": metadata.name,
        "description": metadata.description,
        "version": metadata.version,
        "computeType": "CPU_M",
    }
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


DOCKER_IMAGE_NAME = "datacloud-custom-code"
DEPENDENCIES_ARCHIVE_NAME = "dependencies.tar.gz"
ZIP_FILE_NAME = "deployment.zip"


def prepare_dependency_archive(directory: str) -> None:
    cmd = f"docker images -q {DOCKER_IMAGE_NAME}"
    image_exists = cmd_output(cmd)

    if not image_exists:
        logger.debug("Building docker image...")
        cmd = f"docker build -t {DOCKER_IMAGE_NAME} ."
        cmd_output(cmd)

    with tempfile.TemporaryDirectory() as temp_dir:
        shutil.copy("requirements.txt", temp_dir)
        cmd = (
            f"docker run --rm "
            f"-v {temp_dir}:/dependencies "
            f"{DOCKER_IMAGE_NAME} "
            f'/bin/bash -c "cd /dependencies && pip download -r requirements.txt"'
        )
        cmd_output(cmd)

        archives_dir = os.path.join(directory, "archives")
        os.makedirs(archives_dir, exist_ok=True)
        archive_file = os.path.join(archives_dir, DEPENDENCIES_ARCHIVE_NAME)
        with tarfile.open(archive_file, "w:gz") as tar:
            for file in os.listdir(temp_dir):
                tar.add(os.path.join(temp_dir, file), arcname=file)

        logger.debug(f"Dependencies downloaded and archived to {archive_file}")


def zip_and_upload_directory(directory: str, file_upload_url: str) -> None:
    file_upload_url = unescape(file_upload_url)

    logger.debug(f"Zipping directory... {directory}")
    shutil.make_archive(ZIP_FILE_NAME.rstrip(".zip"), "zip", directory)

    logger.debug(f"Uploading deployment to {file_upload_url}")
    with open(ZIP_FILE_NAME, "rb") as zip_file:
        response = requests.put(
            file_upload_url, data=zip_file, headers={"Content-Type": "application/zip"}
        )
        response.raise_for_status()


class DeploymentsResponse(BaseModel):
    deploymentStatus: str


def get_deployments(
    access_token: AccessTokenResponse, metadata: TransformationJobMetadata
) -> DeploymentsResponse:
    """Get all custom code deployments from the DataCloud."""
    url = _join_strip_url(
        access_token.instance_url, DATA_CUSTOM_CODE_PATH, metadata.name
    )
    response = _make_api_call(url, "GET", token=access_token.access_token)
    return DeploymentsResponse(**response)


def wait_for_deployment(
    access_token: AccessTokenResponse,
    metadata: TransformationJobMetadata,
    callback: Union[Callable[[str], None], None] = None,
) -> None:
    """Wait for deployment to complete.

    Args:
        callback: Optional callback function that receives the deployment status
    """
    start_time = time.time()
    logger.debug("Waiting for deployment to complete")

    while True:
        deployment_status = get_deployments(access_token, metadata)
        status = deployment_status.deploymentStatus
        if (time.time() - start_time) > WAIT_FOR_DEPLOYMENT_TIMEOUT:
            raise TimeoutError("Deployment timed out.")

        if callback:
            callback(status)
        if status == "Deployed":
            logger.debug(
                "Deployment completed, Elapsed time: {time.time() - start_time}"
            )
            break
        time.sleep(1)


DATA_TRANSFORM_REQUEST_TEMPLATE: dict[str, Any] = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v8.json",
        "dbt_version": "1.4.6",
        "generated_at": "2023-04-25T18:54:11.375589Z",
        "invocation_id": "d6c68c69-533a-4d54-861e-1493d6cd8092",
        "env": {},
        "project_id": "jaffle_shop",
        "user_id": "1ca8403c-a1a5-43af-8b88-9265e948b9d2",
        "send_anonymous_usage_stats": True,
        "adapter_type": "spark",
    },
    "nodes": {
        "model.dcexample.dim_listings_w_hosts": {
            "name": "dim_listings_w_hosts",
            "resource_type": "model",
            "relation_name": "{OUTPUT_DLO}",
            "config": {"materialized": "table"},
            "compiled_code": "",
            "depends_on": {"nodes": []},
        }
    },
    "sources": {
        "source.dcexample.listings": {
            "name": "listings",
            "resource_type": "source",
            "relation_name": "{INPUT_DLO}",
            "identifier": "{INPUT_DLO}",
        }
    },
    "macros": {
        "macro.dcexample.byoc": {
            "name": "byoc_example",
            "resource_type": "macro",
            "path": "",
            "original_file_path": "",
            "unique_id": "unique id",
            "macro_sql": "",
            "supported_languages": None,
            "arguments": [{"name": "{SCRIPT_NAME}", "type": "BYOC_SCRIPT"}],
        }
    },
}


class DataTransformConfig(BaseModel):
    input: Union[str, list[str]]
    output: Union[str, list[str]]


DATA_TRANSFORM_CONFIG_TEMPLATE: dict[str, Any] = {
    "entryPoint": "entrypoint.py",
    "dataspace": "default",
    "permissions": {"read": {"dlo": ""}, "write": {"dlo": ""}},
}


def get_data_transform_config(directory: str) -> DataTransformConfig:
    """Get the data transform config from the entrypoint.py file."""
    entrypoint_file = os.path.join(directory, "entrypoint.py")
    data_access_layer_calls = scan_file(entrypoint_file)
    input_ = data_access_layer_calls.input_str
    output = data_access_layer_calls.output_str
    return DataTransformConfig(input=input_, output=output)


def create_data_transform_config(directory: str) -> None:
    """Create a data transform config.json file in the directory."""
    data_transform_config = get_data_transform_config(directory)
    request_hydrated = DATA_TRANSFORM_CONFIG_TEMPLATE.copy()
    request_hydrated["permissions"]["read"]["dlo"] = data_transform_config.input
    request_hydrated["permissions"]["write"]["dlo"] = data_transform_config.output
    logger.debug(f"Creating data transform config in {directory}")
    json.dump(
        request_hydrated, open(os.path.join(directory, "config.json"), "w"), indent=4
    )


def create_data_transform(
    directory: str,
    access_token: AccessTokenResponse,
    metadata: TransformationJobMetadata,
) -> dict:
    """Create a data transform in the DataCloud."""
    script_name = metadata.name
    data_transform_config = get_data_transform_config(directory)
    request_hydrated = DATA_TRANSFORM_REQUEST_TEMPLATE.copy()
    request_hydrated["nodes"]["model.dcexample.dim_listings_w_hosts"][
        "relation_name"
    ] = data_transform_config.input
    request_hydrated["sources"]["source.dcexample.listings"][
        "relation_name"
    ] = data_transform_config.output
    request_hydrated["sources"]["source.dcexample.listings"][
        "identifier"
    ] = data_transform_config.output
    request_hydrated["macros"]["macro.dcexample.byoc"]["arguments"][0][
        "name"
    ] = script_name

    body = {
        "definition": {
            "type": "DBT",
            "manifest": request_hydrated,
            "version": "56.0",
        },
        "label": f"{metadata.name}",
        "name": f"{metadata.name}",
        "type": "BATCH",
    }

    url = _join_strip_url(access_token.instance_url, DATA_TRANSFORMS_PATH)
    response = _make_api_call(url, "POST", token=access_token.access_token, json=body)
    return response


def deploy_full(
    directory: str,
    metadata: TransformationJobMetadata,
    credentials: Credentials,
    callback=None,
) -> AccessTokenResponse:
    """Deploy a data transform in the DataCloud."""
    access_token = _retrieve_access_token(credentials)

    # prepare payload
    prepare_dependency_archive(directory)
    create_data_transform_config(directory)

    # create deployment and upload payload
    deployment = create_deployment(access_token, metadata)
    zip_and_upload_directory(directory, deployment.fileUploadUrl)
    wait_for_deployment(access_token, metadata, callback)

    # create data transform
    create_data_transform(directory, access_token, metadata)
    return access_token


def run_data_transform(
    access_token: AccessTokenResponse, metadata: TransformationJobMetadata
) -> dict:
    logger.debug(f"Triggering data transform {metadata.name}")
    url = _join_strip_url(
        access_token.instance_url, DATA_TRANSFORMS_PATH, metadata.name, "actions", "run"
    )
    return _make_api_call(url, "POST")
