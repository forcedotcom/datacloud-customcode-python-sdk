from __future__ import annotations

import importlib.util
import json
import os
import textwrap

import pytest
import yaml

# Import entry_func directly from its module to avoid pulling in heavy
# SDK dependencies via datacustomcode/__init__.py.
_spec = importlib.util.spec_from_file_location(
    "datacustomcode.entry_func",
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "src",
        "datacustomcode",
        "entry_func.py",
    ),
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]
entry_func = _mod.entry_func

from scripts.generate_byoc_schema import (
    FunctionSchema,
    extract_entry_functions,
    generate_openapi,
    load_config,
    main,
    python_type_to_openapi,
    resolve_config,
)


# ---------------------------------------------------------------------------
# python_type_to_openapi
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# entry_func decorator
# ---------------------------------------------------------------------------


class TestEntryFuncDecorator:
    """Tests for the entry_func decorator itself."""

    def test_decorated_function_is_callable(self):
        @entry_func
        def add(a: int, b: int) -> int:
            return a + b

        assert callable(add)

    def test_decorated_function_returns_correct_result(self):
        @entry_func
        def add(a: int, b: int = 0) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(10) == 10

    def test_preserves_function_name(self):
        @entry_func
        def my_function(x: int) -> int:
            return x

        assert my_function.__name__ == "my_function"

    def test_preserves_docstring(self):
        @entry_func
        def my_function(x: int) -> int:
            """My docstring."""
            return x

        assert my_function.__doc__ == "My docstring."

    def test_schema_extraction_with_sdk_import(self):
        """Source using the SDK import should be extractable."""
        source = textwrap.dedent("""\
            from datacustomcode.entry_func import entry_func

            @entry_func
            def add(a: int, b: int = 0) -> int:
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "add"

    def test_schema_extraction_with_top_level_import(self):
        """Source using top-level SDK import should be extractable."""
        source = textwrap.dedent("""\
            from datacustomcode import entry_func

            @entry_func
            def greet(name: str) -> str:
                return name
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "greet"

    def test_empty_lines_between_decorator_and_function(self):
        """Multiple blank lines between @entry_func and def should still work."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn


            @entry_func


            def add(a: int, b: int = 0) -> int:
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "add"
        assert schemas[0].prototype == "add(a: int, b: int = 0) -> int"


# ---------------------------------------------------------------------------
# python_type_to_openapi
# ---------------------------------------------------------------------------


class TestPythonTypeToOpenapi:
    def test_int(self):
        assert python_type_to_openapi("int") == {"type": "integer"}

    def test_float(self):
        assert python_type_to_openapi("float") == {"type": "number"}

    def test_str(self):
        assert python_type_to_openapi("str") == {"type": "string"}

    def test_bool(self):
        assert python_type_to_openapi("bool") == {"type": "boolean"}

    def test_list(self):
        assert python_type_to_openapi("list") == {"type": "array"}

    def test_dict(self):
        assert python_type_to_openapi("dict") == {"type": "object"}

    def test_unsupported_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            python_type_to_openapi("complex")


# ---------------------------------------------------------------------------
# extract_entry_functions
# ---------------------------------------------------------------------------


class TestExtractEntryFunctions:
    def test_simple_function(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int = 0) -> int:
                \"\"\"Add two integers.\"\"\"
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        s = schemas[0]
        assert s.name == "add"
        assert s.docstring == "Add two integers."
        assert s.return_type == "int"
        assert s.prototype == "add(a: int, b: int = 0) -> int"
        assert len(s.params) == 2
        assert s.params[0]["name"] == "a"
        assert s.params[0]["type"] == "int"
        assert s.params[1]["name"] == "b"
        assert s.params[1]["type"] == "int"
        assert s.params[1]["default"] == 0

    def test_no_entry_func(self):
        source = textwrap.dedent("""\
            def add(a: int, b: int) -> int:
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert schemas == []

    def test_missing_type_annotation_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a, b: int) -> int:
                return a + b
        """)
        with pytest.raises(ValueError, match="missing a type annotation"):
            extract_entry_functions(source)

    def test_all_params_missing_types_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a, b):
                return a + b
        """)
        with pytest.raises(ValueError, match="missing a type annotation"):
            extract_entry_functions(source)

    def test_unsupported_param_type_raises(self):
        """Using an unsupported type like complex should raise on spec generation."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def calc(a: complex) -> int:
                return int(a.real)
        """)
        schemas = extract_entry_functions(source)
        with pytest.raises(ValueError, match="Unsupported type"):
            generate_openapi(schemas)

    def test_unsupported_return_type_raises(self):
        """Using an unsupported return type should raise on spec generation."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def calc(a: int) -> complex:
                return complex(a, 0)
        """)
        schemas = extract_entry_functions(source)
        with pytest.raises(ValueError, match="Unsupported type"):
            generate_openapi(schemas)

    def test_no_return_type(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def greet(name: str):
                \"\"\"Say hello.\"\"\"
                print(f"Hello, {name}")
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].return_type is None
        assert schemas[0].prototype == "greet(name: str)"

    def test_multiple_entry_functions(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int) -> int:
                return a + b

            @entry_func
            def multiply(x: float, y: float) -> float:
                return x * y
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 2
        assert schemas[0].name == "add"
        assert schemas[1].name == "multiply"

    def test_string_default(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def greet(name: str, greeting: str = "hello") -> str:
                return f"{greeting}, {name}"
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].params[1]["default"] == "hello"

    def test_bool_param(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def toggle(flag: bool = True) -> bool:
                return not flag
        """)
        schemas = extract_entry_functions(source)
        p = schemas[0].params[0]
        assert p["name"] == "flag"
        assert p["type"] == "bool"
        assert p["default"] is True

    def test_no_docstring(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int) -> int:
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].docstring is None

    def test_ignores_non_decorated_functions(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            def helper(x: int) -> int:
                return x + 1

            @entry_func
            def add(a: int, b: int) -> int:
                return a + b
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "add"


# ---------------------------------------------------------------------------
# generate_openapi
# ---------------------------------------------------------------------------


class TestGenerateOpenapi:
    def test_basic_spec(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int = 0) -> int:
                \"\"\"Add two integers.\"\"\"
                return a + b
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)

        assert spec["openapi"] == "3.1.0"
        assert spec["info"]["title"] == "Add Service"
        assert "/add" in spec["paths"]

        post = spec["paths"]["/add"]["post"]
        assert post["operationId"] == "add"
        assert post["summary"] == "Add two integers"

        x_fn = post["x-function"]
        assert x_fn["language"] == "python"
        assert x_fn["prototype"] == "add(a: int, b: int = 0) -> int"

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert "a" in req_schema["properties"]
        assert "b" in req_schema["properties"]
        assert req_schema["required"] == ["a"]
        assert req_schema["properties"]["b"]["default"] == 0

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema["properties"]["result"]["type"] == "integer"

    def test_custom_namespace_and_package(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def foo(x: int) -> int:
                return x
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas, namespace="testOrg", package="testPkg")
        x_fn = spec["paths"]["/foo"]["post"]["x-function"]
        assert x_fn["namespace"] == "testOrg"
        assert x_fn["package"] == "testPkg"

    def test_empty_schemas_raises(self):
        with pytest.raises(ValueError, match="No @entry_func"):
            generate_openapi([])

    def test_no_return_type_response(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def do_stuff(x: int):
                pass
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        resp_schema = spec["paths"]["/do_stuff"]["post"]["responses"]["200"][
            "content"
        ]["application/json"]["schema"]
        assert resp_schema == {"type": "object"}

    def test_all_params_have_defaults(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def f(a: int = 1, b: str = "hi") -> str:
                return str(a) + b
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req_schema = spec["paths"]["/f"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert "required" not in req_schema

    def test_400_response_present(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def f(a: int) -> int:
                return a
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        assert "400" in spec["paths"]["/f"]["post"]["responses"]

    def test_output_is_valid_yaml(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int = 0) -> int:
                \"\"\"Add two integers.\"\"\"
                return a + b
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        yaml_str = yaml.dump(spec, default_flow_style=False, sort_keys=False)
        reloaded = yaml.safe_load(yaml_str)
        assert reloaded["openapi"] == "3.1.0"


# ---------------------------------------------------------------------------
# CLI (main)
# ---------------------------------------------------------------------------


class TestMain:
    def test_stdout(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int = 0) -> int:
                \"\"\"Add two integers.\"\"\"
                return a + b
        """))
        assert main([str(src)]) == 0

    def test_output_file(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a: int, b: int = 0) -> int:
                \"\"\"Add two integers.\"\"\"
                return a + b
        """))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())
        assert spec["openapi"] == "3.1.0"
        assert "/add" in spec["paths"]

    def test_no_entry_func_raises(self, tmp_path):
        src = tmp_path / "empty.py"
        src.write_text("x = 1\n")
        with pytest.raises(ValueError, match="No @entry_func"):
            main([str(src)])

    def test_untyped_param_raises(self, tmp_path):
        src = tmp_path / "bad.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(a, b):
                return a + b
        """))
        with pytest.raises(ValueError, match="missing a type annotation"):
            main([str(src)])


# ---------------------------------------------------------------------------
# Complex / nested type tests
# ---------------------------------------------------------------------------


class TestNestedTypes:
    """Tests for generic and nested type annotations like Dict[str, Dict[...]]."""

    def test_dict_str_int_param(self):
        """Dict[str, int] → object with additionalProperties integer."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(data: Dict[str, int]) -> int:
                return sum(data.values())
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["data"]
        assert prop == {"type": "object", "additionalProperties": {"type": "integer"}}

    def test_dict_nested_in_dict(self):
        """Dict[str, Dict[str, int]] → nested object with additionalProperties."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(data: Dict[str, Dict[str, int]]) -> str:
                return "ok"
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["data"]
        assert prop == {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {"type": "integer"},
            },
        }

    def test_triple_nested_dict(self):
        """Dict[str, Dict[str, Dict[str, float]]] → 3 levels of nesting."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def deep(data: Dict[str, Dict[str, Dict[str, float]]]) -> str:
                return "deep"
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/deep"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["data"]
        assert prop == {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "number"},
                },
            },
        }

    def test_list_of_int(self):
        """List[int] → array with items integer."""
        source = textwrap.dedent("""\
            from typing import List
            def entry_func(fn):
                return fn

            @entry_func
            def total(numbers: List[int]) -> int:
                return sum(numbers)
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/total"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["numbers"]
        assert prop == {"type": "array", "items": {"type": "integer"}}

    def test_list_of_dict(self):
        """List[Dict[str, int]] → array of objects."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(records: List[Dict[str, int]]) -> str:
                return "ok"
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["records"]
        assert prop == {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": {"type": "integer"},
            },
        }

    def test_dict_of_list(self):
        """Dict[str, List[int]] → object with array additionalProperties."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(groups: Dict[str, List[int]]) -> str:
                return "ok"
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["groups"]
        assert prop == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }

    def test_nested_return_type(self):
        """Return type Dict[str, List[int]] should be reflected in response."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def group_data(data: List[int]) -> Dict[str, List[int]]:
                return {"evens": [x for x in data if x % 2 == 0]}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        resp = spec["paths"]["/group_data"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["properties"]["result"]
        assert resp == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }

    def test_mixed_simple_and_nested_params(self):
        """Function with both simple and nested params."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def update(name: str, scores: Dict[str, int], verbose: bool = False) -> str:
                return name
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/update"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req["properties"]["name"] == {"type": "string"}
        assert req["properties"]["scores"] == {
            "type": "object",
            "additionalProperties": {"type": "integer"},
        }
        assert req["properties"]["verbose"] == {"type": "boolean", "default": False}
        assert req["required"] == ["name", "scores"]

    def test_prototype_shows_nested_types(self):
        """The prototype string should display full generic type annotations."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(data: Dict[str, List[int]]) -> Dict[str, int]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].prototype == (
            "process(data: Dict[str, List[int]]) -> Dict[str, int]"
        )

    def test_list_of_list(self):
        """List[List[str]] → array of arrays."""
        source = textwrap.dedent("""\
            from typing import List
            def entry_func(fn):
                return fn

            @entry_func
            def matrix(rows: List[List[str]]) -> int:
                return len(rows)
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        prop = spec["paths"]["/matrix"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]["properties"]["rows"]
        assert prop == {
            "type": "array",
            "items": {"type": "array", "items": {"type": "string"}},
        }

    def test_complex_end_to_end_yaml(self, tmp_path):
        """Full round-trip: nested types → YAML file → reload and verify."""
        src = tmp_path / "complex.py"
        src.write_text(textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def analyze(
                config: Dict[str, Dict[str, int]],
                tags: List[str],
                threshold: float = 0.5,
            ) -> Dict[str, List[int]]:
                \"\"\"Analyze data with nested config.\"\"\"
                return {}
        """))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())

        req = spec["paths"]["/analyze"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req["properties"]["config"] == {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {"type": "integer"},
            },
        }
        assert req["properties"]["tags"] == {
            "type": "array",
            "items": {"type": "string"},
        }
        assert req["properties"]["threshold"] == {"type": "number", "default": 0.5}
        assert req["required"] == ["config", "tags"]

        resp = spec["paths"]["/analyze"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["properties"]["result"]
        assert resp == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }


# ---------------------------------------------------------------------------
# Config file & resolve_config tests
# ---------------------------------------------------------------------------

SAMPLE_SOURCE = textwrap.dedent("""\
    def entry_func(fn):
        return fn

    @entry_func
    def add(a: int, b: int = 0) -> int:
        return a + b
""")


class TestLoadConfig:
    """Tests for load_config reading YAML and JSON files."""

    def test_yaml_config(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: acmeOrg\npackage: acmePkg\n")
        result = load_config(str(cfg))
        assert result == {"namespace": "acmeOrg", "package": "acmePkg"}

    def test_json_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"namespace": "jsonOrg", "package": "jsonPkg"}))
        result = load_config(str(cfg))
        assert result == {"namespace": "jsonOrg", "package": "jsonPkg"}

    def test_partial_config_namespace_only(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: partialOrg\n")
        result = load_config(str(cfg))
        assert result == {"namespace": "partialOrg"}

    def test_partial_config_package_only(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("package: partialPkg\n")
        result = load_config(str(cfg))
        assert result == {"package": "partialPkg"}

    def test_extra_keys_ignored(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: ns\npackage: pkg\nversion: 2.0\n")
        result = load_config(str(cfg))
        assert result == {"namespace": "ns", "package": "pkg"}

    def test_invalid_yaml_raises(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("- just a list\n")
        with pytest.raises(ValueError, match="must contain a mapping"):
            load_config(str(cfg))

    def test_invalid_json_raises(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text("[1, 2, 3]")
        with pytest.raises(ValueError, match="must contain a mapping"):
            load_config(str(cfg))


class TestResolveConfig:
    """Tests for resolve_config precedence logic."""

    def test_defaults_when_nothing_provided(self):
        ns, pkg = resolve_config(
            cli_namespace=None, cli_package=None, config_path=None
        )
        assert ns == "myOrg"
        assert pkg == "myPackage"

    def test_config_file_overrides_defaults(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        ns, pkg = resolve_config(
            cli_namespace=None, cli_package=None, config_path=str(cfg)
        )
        assert ns == "fileOrg"
        assert pkg == "filePkg"

    def test_cli_overrides_config_file(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        ns, pkg = resolve_config(
            cli_namespace="cliOrg", cli_package="cliPkg", config_path=str(cfg)
        )
        assert ns == "cliOrg"
        assert pkg == "cliPkg"

    def test_cli_namespace_with_config_package(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        ns, pkg = resolve_config(
            cli_namespace="cliOrg", cli_package=None, config_path=str(cfg)
        )
        assert ns == "cliOrg"
        assert pkg == "filePkg"

    def test_config_namespace_with_cli_package(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        ns, pkg = resolve_config(
            cli_namespace=None, cli_package="cliPkg", config_path=str(cfg)
        )
        assert ns == "fileOrg"
        assert pkg == "cliPkg"

    def test_partial_config_falls_back_to_default(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: onlyOrg\n")
        ns, pkg = resolve_config(
            cli_namespace=None, cli_package=None, config_path=str(cfg)
        )
        assert ns == "onlyOrg"
        assert pkg == "myPackage"


class TestMainWithConfig:
    """End-to-end CLI tests with --config flag."""

    def _write_sample(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(SAMPLE_SOURCE)
        return src

    def test_config_file_yaml(self, tmp_path):
        src = self._write_sample(tmp_path)
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: yamlOrg\npackage: yamlPkg\n")
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out), "-c", str(cfg)]) == 0
        spec = yaml.safe_load(out.read_text())
        x_fn = spec["paths"]["/add"]["post"]["x-function"]
        assert x_fn["namespace"] == "yamlOrg"
        assert x_fn["package"] == "yamlPkg"

    def test_config_file_json(self, tmp_path):
        src = self._write_sample(tmp_path)
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"namespace": "jOrg", "package": "jPkg"}))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out), "-c", str(cfg)]) == 0
        spec = yaml.safe_load(out.read_text())
        x_fn = spec["paths"]["/add"]["post"]["x-function"]
        assert x_fn["namespace"] == "jOrg"
        assert x_fn["package"] == "jPkg"

    def test_cli_overrides_config(self, tmp_path):
        src = self._write_sample(tmp_path)
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        out = tmp_path / "out.yaml"
        assert main([
            str(src), "-o", str(out), "-c", str(cfg),
            "--namespace", "cliOrg", "--package", "cliPkg",
        ]) == 0
        spec = yaml.safe_load(out.read_text())
        x_fn = spec["paths"]["/add"]["post"]["x-function"]
        assert x_fn["namespace"] == "cliOrg"
        assert x_fn["package"] == "cliPkg"

    def test_cli_partial_override(self, tmp_path):
        src = self._write_sample(tmp_path)
        cfg = tmp_path / "config.yaml"
        cfg.write_text("namespace: fileOrg\npackage: filePkg\n")
        out = tmp_path / "out.yaml"
        assert main([
            str(src), "-o", str(out), "-c", str(cfg), "--namespace", "cliOrg",
        ]) == 0
        spec = yaml.safe_load(out.read_text())
        x_fn = spec["paths"]["/add"]["post"]["x-function"]
        assert x_fn["namespace"] == "cliOrg"
        assert x_fn["package"] == "filePkg"

    def test_no_config_uses_defaults(self, tmp_path):
        src = self._write_sample(tmp_path)
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())
        x_fn = spec["paths"]["/add"]["post"]["x-function"]
        assert x_fn["namespace"] == "myOrg"
        assert x_fn["package"] == "myPackage"
