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

"""Utilities for inspecting and working with function entrypoints."""

import ast
import importlib.util
import inspect
import json
import typing
from typing import (
    Any,
    Optional,
    Tuple,
)


def load_function_module(entrypoint_path: str, module_name: str = "function_module"):
    """Load a function entrypoint as a Python module.

    Args:
        entrypoint_path: Path to the entrypoint.py file
        module_name: Name to assign to the module

    Returns:
        The loaded module

    Raises:
        ImportError: If the module cannot be loaded
    """
    spec = importlib.util.spec_from_file_location(module_name, entrypoint_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {entrypoint_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_function_callable(module):
    """Get the 'function' callable from a module.

    Args:
        module: The module to extract the function from

    Returns:
        The function callable

    Raises:
        AttributeError: If module doesn't have a 'function' attribute
    """
    if not hasattr(module, "function"):
        raise AttributeError("Module does not have a 'function' callable")
    return module.function


def get_type_name(type_annotation: Any) -> Optional[str]:
    """Extract the type name from a type annotation.

    Args:
        type_annotation: A type annotation object

    Returns:
        The type name as a string, or None if it cannot be determined
    """
    if type_annotation == inspect.Parameter.empty:
        return None

    if hasattr(type_annotation, "__name__"):
        return type_annotation.__name__

    return str(type_annotation)


def get_function_signature_types(
    function_callable,
) -> Tuple[Optional[Any], Optional[Any], Optional[str], Optional[str]]:
    """Extract request and response types from a function signature.

    Args:
        function_callable: The function to inspect

    Returns:
        Tuple of (request_type, response_type, request_type_name, response_type_name)
        Any of these can be None if not found
    """
    sig = inspect.signature(function_callable)
    params = list(sig.parameters.values())

    request_type = None
    request_type_name = None
    if len(params) >= 1:
        request_type = params[0].annotation
        request_type_name = get_type_name(request_type)

    response_type = sig.return_annotation
    response_type_name = get_type_name(response_type)

    return request_type, response_type, request_type_name, response_type_name


def inspect_function_types_static(
    entrypoint_path: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Inspect function types using static AST parsing (no imports).

    This parses the Python file without executing it, so it doesn't
    require dependencies to be installed.

    Args:
        entrypoint_path: Path to the entrypoint.py file

    Returns:
        Tuple of (request_type_name, response_type_name)
    """
    try:
        with open(entrypoint_path, "r") as f:
            tree = ast.parse(f.read(), filename=entrypoint_path)

        # Find the 'function' definition
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "function":
                # Get request type (first parameter annotation)
                request_type_name = None
                if node.args.args and len(node.args.args) > 0:
                    first_param = node.args.args[0]
                    if first_param.annotation:
                        request_type_name = _get_type_name_from_ast(
                            first_param.annotation
                        )

                # Get response type (return annotation)
                response_type_name = None
                if node.returns:
                    response_type_name = _get_type_name_from_ast(node.returns)

                return request_type_name, response_type_name

        return None, None
    except Exception:
        return None, None


def _get_type_name_from_ast(annotation) -> Optional[str]:
    """Extract type name from an AST annotation node."""
    if isinstance(annotation, ast.Name):
        # Simple type: MyType
        return annotation.id
    elif isinstance(annotation, ast.Attribute):
        # Module.Type - just return the type name
        return annotation.attr
    elif isinstance(annotation, ast.Subscript):
        # Generic type: List[MyType], Optional[MyType]
        # Return the base type name
        return _get_type_name_from_ast(annotation.value)
    return None


def _import_pydantic_model(entrypoint_path: str, type_name: str) -> Optional[Any]:
    """Import a Pydantic model by finding its import statement.

    Parses the entrypoint to find where the type is imported from,
    then imports just that module (not the entrypoint itself).

    Args:
        entrypoint_path: Path to entrypoint.py
        type_name: Name of the type to import (e.g., "SearchIndexChunkingV1Request")

    Returns:
        The Pydantic model class, or None if not found
    """
    try:
        with open(entrypoint_path, "r") as f:
            tree = ast.parse(f.read(), filename=entrypoint_path)

        # Find where this type is imported from
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                # from module import Type1, Type2
                for alias in node.names:
                    if alias.name == type_name:
                        # Found it! Import from the module
                        module_name = node.module
                        if module_name:
                            module = importlib.import_module(module_name)
                            return getattr(module, type_name, None)

        return None
    except Exception:
        return None


def inspect_function_types(
    entrypoint_path: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Inspect a function entrypoint and extract type names.

    Args:
        entrypoint_path: Path to the entrypoint.py file

    Returns:
        Tuple of (request_type_name, response_type_name)
        Either can be None if not found or on error

    Example:
        >>> request_name, response_name = inspect_function_types("payload/entrypoint.py")
        >>> print(request_name)  # "SearchIndexChunkingV1Request"
        >>> print(response_name)  # "SearchIndexChunkingV1Response"
    """
    try:
        module = load_function_module(entrypoint_path, "temp_module")
        function_callable = get_function_callable(module)
        _, _, request_type_name, response_type_name = get_function_signature_types(
            function_callable
        )
        return request_type_name, response_type_name
    except Exception:
        return None, None


def get_request_type(entrypoint_path: str) -> Optional[Any]:
    """Get the request type annotation from a function entrypoint.

    Args:
        entrypoint_path: Path to the entrypoint.py file

    Returns:
        The request type (Pydantic model class), or None if not found

    Raises:
        ImportError: If the module cannot be loaded
        AttributeError: If the function is not found
        ValueError: If the function signature is invalid
    """
    module = load_function_module(entrypoint_path)
    function_callable = get_function_callable(module)

    sig = inspect.signature(function_callable)
    params = list(sig.parameters.values())

    if len(params) < 1:
        raise ValueError("Function must accept at least one parameter (request)")

    request_type = params[0].annotation
    if request_type == inspect.Parameter.empty:
        raise ValueError("Function request parameter must have a type annotation")

    return request_type


def _generate_model_sample_data(model_type):
    """Generate sample data for all fields in a Pydantic model.

    Args:
        model_type: A Pydantic model class

    Returns:
        Dictionary with sample data for all fields
    """
    from pydantic_core import PydanticUndefined

    sample_data = {}
    for field_name, field_info in model_type.model_fields.items():
        # Check if field has a real default value
        if field_info.default is not PydanticUndefined:
            sample_data[field_name] = field_info.default
        else:
            # Required field or field without default - generate sample
            sample_data[field_name] = generate_sample_value(
                field_info.annotation, field_name
            )
    return sample_data


def generate_sample_value(field_type, field_name: str):
    """Generate a sample value based on field type.

    Args:
        field_type: The type annotation of the field
        field_name: The name of the field (used for contextual sample generation)

    Returns:
        A sample value appropriate for the field type
    """
    origin = typing.get_origin(field_type)

    if origin is list or field_type is list:
        args = typing.get_args(field_type)
        if args:
            return [generate_sample_value(args[0], field_name)]
        return []
    elif origin is dict or field_type is dict:
        return {}
    elif field_type is str or origin is typing.Literal:
        if "version" in field_name.lower():
            return "v1"
        return f"sample_{field_name}"
    elif field_type is int:
        if "max" in field_name.lower() or "characters" in field_name.lower():
            return 100
        return 1
    elif field_type is float:
        return 1.0
    elif field_type is bool:
        return True
    elif hasattr(field_type, "model_fields"):
        # Nested Pydantic model - use shared helper
        return _generate_model_sample_data(field_type)
    else:
        return None


def generate_test_json(entrypoint_path: str, output_path: str) -> None:
    """Generate a sample test.json file for a function.

    First tries static AST parsing to get type names, then uses those
    to import only the Pydantic model classes (not the entrypoint).

    Args:
        entrypoint_path: Path to the function entrypoint.py
        output_path: Output path for test.json

    Raises:
        ImportError: If the Pydantic model cannot be loaded
        ValueError: If the request type is not found or not a Pydantic model
    """
    # First, get the type name using static parsing (no imports)
    request_type_name, _ = inspect_function_types_static(entrypoint_path)

    if not request_type_name:
        raise ValueError("Could not determine request type from function signature")

    # Now try to import the Pydantic model class
    # Look for it in the entrypoint's imports
    request_type = _import_pydantic_model(entrypoint_path, request_type_name)

    if not request_type:
        raise ValueError(f"Could not import Pydantic model: {request_type_name}")

    # Check if it's a Pydantic model
    if not hasattr(request_type, "model_fields"):
        raise ValueError("Request parameter type must be a Pydantic model")

    # Generate sample data for ALL fields (use defaults where available)
    sample_data = _generate_model_sample_data(request_type)
    sample_instance = request_type(**sample_data)

    # Write to file
    with open(output_path, "w") as f:
        json.dump(sample_instance.model_dump(), f, indent=2)
