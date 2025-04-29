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
from typing import (
    Dict,
    List,
    Union,
)

import pydantic

DATA_ACCESS_METHODS = ["read_dlo", "read_dmo", "write_to_dlo", "write_to_dmo"]


class DataAccessLayerCalls(pydantic.BaseModel):
    read_dlo: frozenset[str]
    read_dmo: frozenset[str]
    write_to_dlo: frozenset[str]
    write_to_dmo: frozenset[str]

    @pydantic.model_validator(mode="after")
    def validate_access_layer(self) -> DataAccessLayerCalls:
        if self.read_dlo and self.read_dmo:
            raise ValueError("Cannot read from DLO and DMO in the same file.")
        if len(self.write_to_dlo) > 1 or len(self.write_to_dmo) > 1:
            raise ValueError(
                "Cannot write to more than one DLO or DMO in the same file."
            )
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


def scan_file(file_path: str) -> DataAccessLayerCalls:
    """Scan a single Python file for Client read/write method calls."""
    with open(file_path, "r") as f:
        code = f.read()
        tree = ast.parse(code)
        visitor = ClientMethodVisitor()
        visitor.visit(tree)
        return visitor.found()


def dc_config_json_from_file(file_path: str) -> dict:
    """Create a Data Cloud Custom Code config JSON from a script."""
    output = scan_file(file_path)
    read = {}
    if output.read_dlo:
        read["dlo"] = list(output.read_dlo)
    else:
        read["dmo"] = list(output.read_dmo)
    write = {}
    if output.write_to_dlo:
        write["dlo"] = list(output.write_to_dlo)
    else:
        write["dmo"] = list(output.write_to_dmo)
    config: Dict[str, Union[str, Dict[str, Dict[str, List[str]]]]] = {
        "entryPoint": file_path.rpartition("/")[-1],
        "dataspace": "default",
        "permissions": {
            "read": read,
            "write": write,
        },
    }
    return config
