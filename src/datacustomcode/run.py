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
import importlib
import json
import os
from pathlib import Path
import runpy
import sys
from typing import List, Union

from datacustomcode.config import config


def run_entrypoint(
    entrypoint: str,
    config_file: Union[str, None],
    dependencies: List[str],
    profile: str,
) -> None:
    """Run the entrypoint script with the given config and dependencies.

    Args:
        entrypoint: The entrypoint script to run.
        config_file: The config file to use.
        dependencies: The dependencies to import.
        profile: The credentials profile to use.
    """
    add_py_folder(entrypoint)

    # Read dataspace from config.json (required)
    entrypoint_dir = os.path.dirname(entrypoint)
    config_json_path = os.path.join(entrypoint_dir, "config.json")

    if not os.path.exists(config_json_path):
        raise FileNotFoundError(
            f"config.json not found at {config_json_path}. config.json is required."
        )

    try:
        with open(config_json_path, "r") as f:
            config_json = json.load(f)
    except json.JSONDecodeError as err:
        raise ValueError(
            f"config.json at {config_json_path} is not valid JSON"
        ) from err

    # Require dataspace to be present in config.json
    dataspace = config_json.get("dataspace")
    if not dataspace:
        raise ValueError(
            f"config.json at {config_json_path} is missing required field 'dataspace'. "
            f"Please ensure config.json contains a 'dataspace' field."
        )

    # Load config file first (if provided) so that dataspace from config.json
    # can override any dataspace in the config file
    if config_file:
        config.load(config_file)

    # Add dataspace to reader config options
    # (after loading config file to ensure it takes precedence)
    if config.reader_config and hasattr(config.reader_config, "options"):
        config.reader_config.options["dataspace"] = dataspace
    # Add dataspace to writer config options (for PrintDataCloudWriter)
    if config.writer_config and hasattr(config.writer_config, "options"):
        config.writer_config.options["dataspace"] = dataspace

    if profile != "default":
        if config.reader_config and hasattr(config.reader_config, "options"):
            config.reader_config.options["credentials_profile"] = profile
        if config.writer_config and hasattr(config.writer_config, "options"):
            config.writer_config.options["credentials_profile"] = profile
    for dependency in dependencies:
        try:
            importlib.import_module(dependency)
        except ModuleNotFoundError as exc:
            try:
                if "." in dependency:
                    module_name, object_name = dependency.rsplit(".", 1)
                    module = importlib.import_module(module_name)
                    getattr(module, object_name)
                else:
                    raise exc
            except (ModuleNotFoundError, AttributeError) as inner_exc:
                raise inner_exc from exc
    runpy.run_path(entrypoint, init_globals=globals(), run_name="__main__")


def add_py_folder(entrypoint: str):
    default_py_folder = "py-files"  # Hardcoded folder name
    cwd = Path.cwd().joinpath(entrypoint)
    py_folder = cwd.parent.joinpath(default_py_folder)

    sys.path.append(str(py_folder))
