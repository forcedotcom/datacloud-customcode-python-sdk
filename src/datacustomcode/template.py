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
import os
import shutil
from typing import Optional

from loguru import logger

from datacustomcode.constants import FEATURE_TEMPLATE_MAPPING

script_template_dir = os.path.join(os.path.dirname(__file__), "templates", "script")
function_template_dir = os.path.join(os.path.dirname(__file__), "templates", "function")

STREAMING_EXAMPLE_ENTRYPOINT = os.path.join(
    script_template_dir, "examples", "streaming_deltas", "entrypoint.py"
)

_SDK_CONFIG_YAML = os.path.join(os.path.dirname(__file__), "config.yaml")


def copy_script_template(target_dir: str, streaming: bool = False) -> None:
    """Copy the template to the target directory."""
    os.makedirs(target_dir, exist_ok=True)

    for item in os.listdir(script_template_dir):
        source = os.path.join(script_template_dir, item)
        destination = os.path.join(target_dir, item)

        if os.path.isdir(source):
            logger.debug(f"Copying directory {source} to {destination}...")
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            logger.debug(f"Copying file {source} to {destination}...")
            shutil.copy2(source, destination)

    if streaming:
        destination = os.path.join(target_dir, "payload", "entrypoint.py")
        logger.debug(
            f"Copying streaming example {STREAMING_EXAMPLE_ENTRYPOINT} to "
            f"{destination}..."
        )
        shutil.copy2(STREAMING_EXAMPLE_ENTRYPOINT, destination)

        _write_streaming_config_yaml(target_dir)


def _write_streaming_config_yaml(target_dir: str) -> None:
    """Writes the sdk config file with Streaming overrdies"""
    import yaml

    with open(_SDK_CONFIG_YAML) as f:
        config_data = yaml.safe_load(f)

    config_data["reader_config"]["type_config_name"] = "LocalDeltasReader"
    config_data["writer_config"]["type_config_name"] = "LocalDeltasWriter"

    destination = os.path.join(target_dir, "config.yaml")
    logger.debug(f"Writing streaming config.yaml to {destination}...")
    with open(destination, "w") as f:
        yaml.safe_dump(config_data, f, sort_keys=False)


def copy_function_template(target_dir: str, use_in_feature: Optional[str]) -> None:
    os.makedirs(target_dir, exist_ok=True)

    # First, copy common files from base function template
    for item in os.listdir(function_template_dir):
        source = os.path.join(function_template_dir, item)
        destination = os.path.join(target_dir, item)

        # Skip feature-specific subdirectories
        if os.path.isdir(source) and item in FEATURE_TEMPLATE_MAPPING.values():
            continue

        if os.path.isdir(source):
            logger.debug(f"Copying directory {source} to {destination}...")
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            logger.debug(f"Copying file {source} to {destination}...")
            shutil.copy2(source, destination)

    # Then, copy feature-specific files (overwriting if needed)
    if use_in_feature and use_in_feature in FEATURE_TEMPLATE_MAPPING:
        feature_function_template_dir = os.path.join(
            function_template_dir, FEATURE_TEMPLATE_MAPPING[use_in_feature]
        )

        for item in os.listdir(feature_function_template_dir):
            source = os.path.join(feature_function_template_dir, item)
            destination = os.path.join(target_dir, item)

            if os.path.isdir(source):
                logger.debug(
                    f"Copying feature-specific directory {source} to {destination}..."
                )
                shutil.copytree(source, destination, dirs_exist_ok=True)
            else:
                logger.debug(
                    f"Copying feature-specific file {source} to {destination}..."
                )
                shutil.copy2(source, destination)
