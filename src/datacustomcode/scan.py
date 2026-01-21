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

import ast
import json
import os
import sys
from typing import (
    Any,
    ClassVar,
    Dict,
    Set,
    Union,
)

from loguru import logger
import pydantic

from datacustomcode.version import get_version

DATA_ACCESS_METHODS = ["read_dlo", "read_dmo", "write_to_dlo", "write_to_dmo"]

DATA_TRANSFORM_CONFIG_TEMPLATE = {
    "sdkVersion": get_version(),
    "entryPoint": "",
    "dataspace": "default",
    "permissions": {
        "read": {},
        "write": {},
    },
}

FUNCTION_CONFIG_TEMPLATE = {
    "entryPoint": "",
}
STANDARD_LIBS = set(sys.stdlib_module_names)

SDK_CONFIG_DIR = ".datacustomcode_proj"
SDK_CONFIG_FILE = "sdk_config.json"


def get_sdk_config_path(base_directory: str) -> str:
    """Get the path to the SDK-specific config file.

    Args:
        base_directory: The base directory of the project
            (where .datacustomcode should be)

    Returns:
        The path to the SDK config file
    """
    sdk_config_dir = os.path.join(base_directory, SDK_CONFIG_DIR)
    return os.path.join(sdk_config_dir, SDK_CONFIG_FILE)


def read_sdk_config(base_directory: str) -> dict[str, Any]:
    """Read the SDK-specific config file.

    Args:
        base_directory: The base directory of the project

    Returns:
        The SDK config dictionary, or empty dict if file doesn't exist
    """
    config_path = get_sdk_config_path(base_directory)
    if os.path.exists(config_path) and os.path.isfile(config_path):
        try:
            with open(config_path, "r") as f:
                config_data: dict[str, Any] = json.load(f)
                return config_data
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from {config_path}: {e}") from e
        except OSError as e:
            raise OSError(f"Failed to read SDK config file {config_path}: {e}") from e
    else:
        raise FileNotFoundError(f"SDK config file not found at {config_path}")


def write_sdk_config(base_directory: str, config: dict[str, Any]) -> None:
    """Write the SDK-specific config file.

    Args:
        base_directory: The base directory of the project
        config: The config dictionary to write
    """
    config_path = get_sdk_config_path(base_directory)
    sdk_config_dir = os.path.dirname(config_path)
    os.makedirs(sdk_config_dir, exist_ok=True)
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def get_package_type(base_directory: str) -> str:
    """Get the package type (script or function) from SDK config.

    Args:
        base_directory: The base directory of the project

    Returns:
        The package type ("script" or "function")

    Raises:
        ValueError: If the type is not found in the SDK config
    """
    try:
        sdk_config = read_sdk_config(base_directory)
    except FileNotFoundError as e:
        logger.debug(f"Defaulting to script package type. {e}")
        return "script"
    if "type" not in sdk_config:
        config_path = get_sdk_config_path(base_directory)
        raise ValueError(
            f"Package type not found in SDK config at {config_path}. "
            "Please run 'datacustomcode init' to initialize the project."
        )
    return str(sdk_config["type"])


class DataAccessLayerCalls(pydantic.BaseModel):
    read_dlo: frozenset[str]
    read_dmo: frozenset[str]
    write_to_dlo: frozenset[str]
    write_to_dmo: frozenset[str]

    @pydantic.model_validator(mode="after")
    def validate_access_layer(self) -> DataAccessLayerCalls:
        if self.read_dlo and self.read_dmo:
            raise ValueError("Cannot read from DLO and DMO in the same file.")
        if not self.read_dlo and not self.read_dmo:
            raise ValueError("Must read from at least one DLO or DMO.")
        if self.read_dlo and self.write_to_dmo:
            raise ValueError("Cannot read from DLO and write to DMO in the same file.")
        if self.read_dmo and self.write_to_dlo:
            raise ValueError("Cannot read from DMO and write to DLO in the same file.")
        return self

    @property
    def input_str(self) -> str:
        if self.read_dlo:
            return next(iter(self.read_dlo))
        return next(iter(self.read_dmo))

    @property
    def output_str(self) -> str:
        if self.write_to_dlo:
            return next(iter(self.write_to_dlo))
        return next(iter(self.write_to_dmo))


class ClientMethodVisitor(ast.NodeVisitor):
    """AST Visitor that finds all instances of Client read/write method calls."""

    def __init__(self) -> None:
        self._read_dlo_instances: set[str] = set()
        self._read_dmo_instances: set[str] = set()
        self._write_to_dlo_instances: set[str] = set()
        self._write_to_dmo_instances: set[str] = set()
        self.variable_values: Dict[str, Union[str, None]] = {}

    def visit_Assign(self, node: ast.Assign) -> None:
        """Track variable assignments that might be used as DLO/DMO names."""
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id
                if isinstance(node.value, ast.Constant) and isinstance(
                    node.value.value, str
                ):
                    self.variable_values[var_name] = node.value.value
                else:
                    self.variable_values[var_name] = None
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Visit a method call and check if it's a Client read/write method."""
        if isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Name
        ):
            method_name = node.func.attr
            if method_name in DATA_ACCESS_METHODS and node.args:
                arg = node.args[0]
                name = None

                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    name = arg.value
                elif isinstance(arg, ast.Name) and arg.id in self.variable_values:
                    name = self.variable_values[arg.id]

                if name:
                    if method_name == "read_dlo":
                        self._read_dlo_instances.add(name)
                    elif method_name == "read_dmo":
                        self._read_dmo_instances.add(name)
                    elif method_name == "write_to_dlo":
                        self._write_to_dlo_instances.add(name)
                    elif method_name == "write_to_dmo":
                        self._write_to_dmo_instances.add(name)
        self.generic_visit(node)

    def found(self) -> DataAccessLayerCalls:
        return DataAccessLayerCalls(
            read_dlo=frozenset(self._read_dlo_instances),
            read_dmo=frozenset(self._read_dmo_instances),
            write_to_dlo=frozenset(self._write_to_dlo_instances),
            write_to_dmo=frozenset(self._write_to_dmo_instances),
        )


class ImportVisitor(ast.NodeVisitor):
    """AST Visitor that extracts external package imports from Python code."""

    # Additional packages to exclude from requirements.txt
    EXCLUDED_PACKAGES: ClassVar[set[str]] = {
        "datacustomcode",  # Internal package
        "pyspark",  # Provided by the runtime environment
    }

    def __init__(self) -> None:
        self.imports: Set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        """Visit an import statement (e.g., import os, sys)."""
        for name in node.names:
            # Get the top-level package name
            package = name.name.split(".")[0]
            if (
                package not in STANDARD_LIBS
                and package not in self.EXCLUDED_PACKAGES
                and not package.startswith("_")
            ):
                self.imports.add(package)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Visit a from-import statement (e.g., from os import path)."""
        if node.module is not None:
            # Get the top-level package
            package = node.module.split(".")[0]
            if (
                package not in STANDARD_LIBS
                and package not in self.EXCLUDED_PACKAGES
                and not package.startswith("_")
            ):
                self.imports.add(package)
        self.generic_visit(node)


def scan_file_for_imports(file_path: str) -> Set[str]:
    """Scan a Python file for external package imports."""
    with open(file_path, "r") as f:
        code = f.read()
        tree = ast.parse(code)
        visitor = ImportVisitor()
        visitor.visit(tree)
        return visitor.imports


def write_requirements_file(file_path: str) -> str:
    """
    Scan a Python file for imports and write them to requirements.txt.

    Args:
        file_path: Path to the Python file to scan

    Returns:
        Path to the generated requirements.txt file
    """
    imports = scan_file_for_imports(file_path)

    # Write requirements.txt in the parent directory of the Python file
    file_dir = os.path.dirname(file_path)
    parent_dir = os.path.dirname(file_dir) if file_dir else "."
    requirements_path = os.path.join(parent_dir, "requirements.txt")

    # If the file exists, read existing requirements and merge with new ones
    existing_requirements = set()
    if os.path.exists(requirements_path):
        with open(requirements_path, "r") as f:
            existing_requirements = {line.strip() for line in f if line.strip()}

    # Merge existing requirements with newly discovered ones
    all_requirements = existing_requirements.union(imports)

    # Write the combined requirements
    with open(requirements_path, "w") as f:
        for package in sorted(all_requirements):
            f.write(f"{package}\n")

    return requirements_path


def scan_file(file_path: str) -> DataAccessLayerCalls:
    """Scan a single Python file for Client read/write method calls."""
    with open(file_path, "r") as f:
        code = f.read()
        tree = ast.parse(code)
        visitor = ClientMethodVisitor()
        visitor.visit(tree)
        return visitor.found()


def dc_config_json_from_file(file_path: str, type: str) -> dict[str, Any]:
    """Create a Data Cloud Custom Code config JSON from a script."""
    config: dict[str, Any]
    if type == "script":
        config = DATA_TRANSFORM_CONFIG_TEMPLATE.copy()
    elif type == "function":
        config = FUNCTION_CONFIG_TEMPLATE.copy()
    config["entryPoint"] = file_path.rpartition("/")[-1]
    return config


def find_base_directory(file_path: str) -> str:
    """Find the base directory containing .datacustomcode by walking up from file_path.

    Args:
        file_path: Path to a file in the project

    Returns:
        The base directory path, or the directory containing the file if not found
    """
    current_dir = os.path.dirname(os.path.abspath(file_path))
    root = os.path.abspath(os.sep)

    while current_dir != root:
        if os.path.exists(os.path.join(current_dir, SDK_CONFIG_DIR)):
            return current_dir
        current_dir = os.path.dirname(current_dir)

    # If not found, assume the payload directory's parent is the base
    # (payload/entrypoint.py -> base directory is parent of payload)
    file_dir = os.path.dirname(os.path.abspath(file_path))
    if os.path.basename(file_dir) == "payload":
        return os.path.dirname(file_dir)
    return file_dir


def update_config(file_path: str) -> dict[str, Any]:
    file_dir = os.path.dirname(file_path)
    config_json_path = os.path.join(file_dir, "config.json")
    existing_config: dict[str, Any]
    if os.path.exists(config_json_path) and os.path.isfile(config_json_path):
        try:
            with open(config_json_path, "r") as f:
                existing_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse JSON from {config_json_path}: {e}"
            ) from e
        except OSError as e:
            raise OSError(f"Failed to read config file {config_json_path}: {e}") from e
    else:
        raise ValueError(f"config.json not found at {config_json_path}")

    # Get package type from SDK config
    base_directory = find_base_directory(file_path)
    package_type = get_package_type(base_directory)

    if package_type == "script":
        existing_config["dataspace"] = get_dataspace(existing_config)
        output = scan_file(file_path)
        read: dict[str, list[str]] = {}
        if output.read_dlo:
            read["dlo"] = list(output.read_dlo)
        else:
            read["dmo"] = list(output.read_dmo)
        write: dict[str, list[str]] = {}
        if output.write_to_dlo:
            write["dlo"] = list(output.write_to_dlo)
        else:
            write["dmo"] = list(output.write_to_dmo)

        existing_config["permissions"] = {"read": read, "write": write}
    return existing_config


def get_dataspace(existing_config: dict[str, str]) -> str:
    if "dataspace" in existing_config:
        dataspace_value = existing_config["dataspace"]
        if not dataspace_value or (
            isinstance(dataspace_value, str) and dataspace_value.strip() == ""
        ):
            logger.warning(
                "dataspace is empty or None. "
                "Updating config file to use dataspace 'default'. "
            )
            return "default"
        else:
            return dataspace_value
    else:
        raise ValueError(
            "dataspace must be defined. "
            "Please add a 'dataspace' field to the config.json file. "
        )
