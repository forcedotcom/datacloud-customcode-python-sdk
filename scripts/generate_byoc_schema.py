"""BYOC (Bring Your Own Code) - Extract function schema from a Python file and generate an OpenAPI spec.

Scans a Python file for functions decorated with ``@entry_func``, extracts
their signatures (including type annotations and defaults), and produces an
OpenAPI 3.1.0 specification exposing each function as a POST endpoint.

The ``@entry_func`` decorator is provided by the SDK::

    from datacustomcode.entry_func import entry_func

    @entry_func
    def add(a: int, b: int = 0) -> int:
        return a + b

Usage::

    # Basic usage (uses default namespace="myOrg", package="myPackage")
    python scripts/generate_byoc_schema.py input.py -o openapi.yaml

    # Set namespace and package via command-line flags
    python scripts/generate_byoc_schema.py input.py -o openapi.yaml --namespace myOrg --package myPackage

    # Set namespace and package via a YAML or JSON config file
    python scripts/generate_byoc_schema.py input.py -o openapi.yaml -c config.yaml

    # CLI flags override config file values
    python scripts/generate_byoc_schema.py input.py -o openapi.yaml -c config.yaml --namespace overrideOrg

Config file format (YAML)::

    namespace: myOrg
    package: myPackage

Config file format (JSON)::

    {"namespace": "myOrg", "package": "myPackage"}

Precedence: CLI flags > config file > built-in defaults.
"""

from __future__ import annotations

import argparse
import ast
import inspect
import sys
import textwrap
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

import yaml


# ---------------------------------------------------------------------------
# Python‑type → OpenAPI type mapping
# ---------------------------------------------------------------------------

_SIMPLE_TYPE_MAP: Dict[str, Dict[str, str]] = {
    "int": {"type": "integer"},
    "float": {"type": "number"},
    "str": {"type": "string"},
    "bool": {"type": "boolean"},
}

# Container base names (both builtins and typing module)
_CONTAINER_NAMES = {"list", "List", "dict", "Dict"}


def python_type_to_openapi(type_annotation: Any) -> Dict[str, Any]:
    """Convert a Python type annotation (str or AST node) to an OpenAPI schema dict.

    Supports simple types (int, str, …), generic containers
    (``List[int]``, ``Dict[str, int]``), and arbitrarily nested
    combinations (``Dict[str, Dict[str, List[int]]]``).
    """
    # --- AST Subscript node: e.g. Dict[str, int] ---------------------------
    if isinstance(type_annotation, ast.Subscript):
        return _ast_node_to_openapi(type_annotation)

    # --- plain string path (backward compat & simple types) -----------------
    if isinstance(type_annotation, str):
        if type_annotation in _SIMPLE_TYPE_MAP:
            return dict(_SIMPLE_TYPE_MAP[type_annotation])
        if type_annotation in ("list", "List"):
            return {"type": "array"}
        if type_annotation in ("dict", "Dict"):
            return {"type": "object"}
        raise ValueError(f"Unsupported type annotation: {type_annotation}")

    # --- AST Name node (simple, non-generic) --------------------------------
    if isinstance(type_annotation, ast.Name):
        return python_type_to_openapi(type_annotation.id)

    raise ValueError(f"Unsupported type annotation: {ast.dump(type_annotation)}")


def _ast_node_to_openapi(node: ast.expr) -> Dict[str, Any]:
    """Recursively convert an AST annotation node to an OpenAPI schema."""
    if isinstance(node, ast.Name):
        return python_type_to_openapi(node.id)

    if isinstance(node, ast.Constant):
        return python_type_to_openapi(type(node.value).__name__)

    if isinstance(node, ast.Subscript):
        base = node.value
        if not isinstance(base, ast.Name):
            raise ValueError(f"Unsupported generic base: {ast.dump(base)}")
        base_name = base.id

        # --- List[X] / list[X] ---------------------------------------------
        if base_name in ("List", "list"):
            items_schema = _ast_node_to_openapi(node.slice)
            return {"type": "array", "items": items_schema}

        # --- Dict[K, V] / dict[K, V] ---------------------------------------
        if base_name in ("Dict", "dict"):
            if isinstance(node.slice, ast.Tuple):
                elts = node.slice.elts
                if len(elts) != 2:
                    raise ValueError(
                        f"Dict requires exactly 2 type args, got {len(elts)}"
                    )
                value_schema = _ast_node_to_openapi(elts[1])
                return {
                    "type": "object",
                    "additionalProperties": value_schema,
                }
            # Single subscript (unusual but handle gracefully)
            value_schema = _ast_node_to_openapi(node.slice)
            return {"type": "object", "additionalProperties": value_schema}

        raise ValueError(f"Unsupported generic type: {base_name}")

    raise ValueError(f"Unsupported annotation node: {ast.dump(node)}")


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _get_decorator_names(node: ast.FunctionDef) -> List[str]:
    """Return the simple names of all decorators on *node*."""
    names: List[str] = []
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name):
            names.append(dec.id)
        elif isinstance(dec, ast.Attribute):
            names.append(dec.attr)
    return names


def _resolve_default(node: ast.expr) -> Any:
    """Try to turn an AST default‑value node into a Python literal."""
    return ast.literal_eval(node)


def _annotation_to_str(node: ast.expr) -> str:
    """Return the source text of an annotation node, including generics."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Constant):
        return type(node.value).__name__
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        base = _annotation_to_str(node.value)
        if isinstance(node.slice, ast.Tuple):
            args = ", ".join(_annotation_to_str(e) for e in node.slice.elts)
        else:
            args = _annotation_to_str(node.slice)
        return f"{base}[{args}]"
    return ast.dump(node)


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


class FunctionSchema:
    """Parsed representation of a single ``@entry_func`` function."""

    def __init__(
        self,
        name: str,
        docstring: Optional[str],
        params: List[Dict[str, Any]],
        return_type: Optional[str],
        return_type_node: Optional[ast.expr],
        prototype: str,
    ) -> None:
        self.name = name
        self.docstring = docstring
        self.params = params
        self.return_type = return_type
        self.return_type_node = return_type_node
        self.prototype = prototype


def extract_entry_functions(source: str) -> List[FunctionSchema]:
    """Parse *source* and return schemas for every ``@entry_func`` function.

    Raises ``ValueError`` when a decorated function has untyped parameters.
    """
    tree = ast.parse(source)
    results: List[FunctionSchema] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if "entry_func" not in _get_decorator_names(node):
            continue

        # --- validate all params are typed ----------------------------------
        args = node.args
        all_args = args.args
        defaults = args.defaults
        # right‑align defaults to params
        num_no_default = len(all_args) - len(defaults)

        params: List[Dict[str, Any]] = []
        proto_parts: List[str] = []

        for idx, arg in enumerate(all_args):
            if arg.annotation is None:
                raise ValueError(
                    f"Parameter '{arg.arg}' of function '{node.name}' "
                    "is missing a type annotation"
                )
            type_str = _annotation_to_str(arg.annotation)
            param: Dict[str, Any] = {
                "name": arg.arg,
                "type": type_str,
                "type_node": arg.annotation,
            }
            proto = f"{arg.arg}: {type_str}"

            default_idx = idx - num_no_default
            if default_idx >= 0:
                default_val = _resolve_default(defaults[default_idx])
                param["default"] = default_val
                proto += f" = {default_val!r}"

            params.append(param)
            proto_parts.append(proto)

        # --- return type ----------------------------------------------------
        return_type: Optional[str] = None
        return_type_node: Optional[ast.expr] = None
        if node.returns is not None:
            return_type = _annotation_to_str(node.returns)
            return_type_node = node.returns

        ret_str = f" -> {return_type}" if return_type else ""
        prototype = f"{node.name}({', '.join(proto_parts)}){ret_str}"

        # --- docstring ------------------------------------------------------
        docstring = ast.get_docstring(node)

        results.append(
            FunctionSchema(
                name=node.name,
                docstring=docstring,
                params=params,
                return_type=return_type,
                return_type_node=return_type_node,
                prototype=prototype,
            )
        )

    return results


# ---------------------------------------------------------------------------
# OpenAPI generation
# ---------------------------------------------------------------------------


def _title_from_name(name: str) -> str:
    """Convert a snake_case function name to a Title Case service name."""
    return name.replace("_", " ").title() + " Service"


def _build_request_schema(params: List[Dict[str, Any]]) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    required: List[str] = []

    for p in params:
        type_source = p.get("type_node", p["type"])
        prop = python_type_to_openapi(type_source)
        if "default" in p:
            prop["default"] = p["default"]
        else:
            required.append(p["name"])
        properties[p["name"]] = prop

    schema: Dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _build_response_schema(schema: FunctionSchema) -> Dict[str, Any]:
    if schema.return_type is None:
        return {"type": "object"}
    type_source = schema.return_type_node or schema.return_type
    inner = python_type_to_openapi(type_source)
    return {"type": "object", "properties": {"result": inner}}


def generate_openapi(
    schemas: List[FunctionSchema],
    *,
    namespace: str = "myOrg",
    package: str = "myPackage",
) -> Dict[str, Any]:
    """Build an OpenAPI 3.1.0 dict from a list of ``FunctionSchema`` objects."""
    if not schemas:
        raise ValueError("No @entry_func functions found in the input file")

    first = schemas[0]
    spec: Dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {
            "title": _title_from_name(first.name),
            "description": "API generated from Python function schema",
            "version": "1.0.0",
        },
        "paths": {},
    }

    for schema in schemas:
        summary = (schema.docstring or "").split("\n")[0].rstrip(".")
        path_op: Dict[str, Any] = {
            "summary": summary,
            "description": schema.docstring or "",
            "operationId": schema.name,
            "x-function": {
                "language": "python",
                "namespace": namespace,
                "package": package,
                "name": f"{schema.name}Fn",
                "prototype": schema.prototype,
                "description": f"Will perform {schema.name} operation",
            },
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": _build_request_schema(schema.params),
                    }
                },
            },
            "responses": {
                "200": {
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "schema": _build_response_schema(schema),
                        }
                    },
                },
                "400": {"description": "Invalid input"},
            },
        }
        spec["paths"][f"/{schema.name}"] = {"post": path_op}

    return spec


# ---------------------------------------------------------------------------
# Config file support
# ---------------------------------------------------------------------------

_DEFAULT_NAMESPACE = "myOrg"
_DEFAULT_PACKAGE = "myPackage"


def load_config(config_path: str) -> Dict[str, str]:
    """Load namespace/package from a YAML or JSON config file.

    The file may contain any keys; only ``namespace`` and ``package`` are used.
    Supports both YAML (``.yaml``/``.yml``) and JSON (``.json``) formats.
    """
    with open(config_path, "r") as f:
        raw = f.read()

    if config_path.endswith(".json"):
        import json

        data = json.loads(raw)
    else:
        data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a mapping, got {type(data).__name__}")
    return {k: v for k, v in data.items() if k in ("namespace", "package")}


def resolve_config(
    *,
    cli_namespace: Optional[str],
    cli_package: Optional[str],
    config_path: Optional[str],
) -> Tuple[str, str]:
    """Determine final namespace and package values.

    Precedence (highest → lowest):
      1. Command-line flags (``--namespace`` / ``--package``)
      2. Config file (``--config``)
      3. Built-in defaults
    """
    file_ns: Optional[str] = None
    file_pkg: Optional[str] = None

    if config_path is not None:
        cfg = load_config(config_path)
        file_ns = cfg.get("namespace")
        file_pkg = cfg.get("package")

    namespace = cli_namespace or file_ns or _DEFAULT_NAMESPACE
    package = cli_package or file_pkg or _DEFAULT_PACKAGE
    return namespace, package


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate OpenAPI spec from @entry_func functions."
    )
    parser.add_argument("input", help="Path to the Python source file")
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output YAML file (default: stdout)",
    )
    parser.add_argument(
        "--namespace",
        default=None,
        help="Function namespace (overrides config file)",
    )
    parser.add_argument(
        "--package",
        default=None,
        help="Function package (overrides config file)",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to a YAML or JSON config file with namespace/package",
    )

    args = parser.parse_args(argv)

    namespace, package = resolve_config(
        cli_namespace=args.namespace,
        cli_package=args.package,
        config_path=args.config,
    )

    with open(args.input, "r") as f:
        source = f.read()

    schemas = extract_entry_functions(source)
    spec = generate_openapi(schemas, namespace=namespace, package=package)
    yaml_str = yaml.dump(spec, default_flow_style=False, sort_keys=False)

    if args.output:
        with open(args.output, "w") as f:
            f.write(yaml_str)
    else:
        sys.stdout.write(yaml_str)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
