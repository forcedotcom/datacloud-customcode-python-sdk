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
# entry_func decorator
# ---------------------------------------------------------------------------


class TestEntryFuncDecorator:
    """Tests for the entry_func decorator itself."""

    def test_decorated_function_is_callable(self):
        @entry_func
        def process(request: dict) -> dict:
            return request

        assert callable(process)

    def test_decorated_function_returns_correct_result(self):
        @entry_func
        def add(request: dict) -> dict:
            return {"result": request.get("a", 0) + request.get("b", 0)}

        assert add({"a": 2, "b": 3}) == {"result": 5}
        assert add({}) == {"result": 0}

    def test_preserves_function_name(self):
        @entry_func
        def my_function(request: dict) -> dict:
            return request

        assert my_function.__name__ == "my_function"

    def test_preserves_docstring(self):
        @entry_func
        def my_function(request: dict) -> dict:
            """My docstring."""
            return request

        assert my_function.__doc__ == "My docstring."

    def test_schema_extraction_with_sdk_import(self):
        """Source using the SDK import should be extractable."""
        source = textwrap.dedent("""\
            from datacustomcode.entry_func import entry_func

            @entry_func
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_schema_extraction_with_top_level_import(self):
        """Source using top-level SDK import should be extractable."""
        source = textwrap.dedent("""\
            from datacustomcode import entry_func

            @entry_func
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_empty_lines_between_decorator_and_function(self):
        """Multiple blank lines between @entry_func and def should still work."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn


            @entry_func


            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"
        assert schemas[0].prototype == "process(request: dict) -> dict"


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
# extract_entry_functions – valid cases
# ---------------------------------------------------------------------------


class TestExtractEntryFunctions:
    def test_bare_dict(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        s = schemas[0]
        assert s.name == "process"
        assert s.docstring == "Process a request."
        assert s.return_type == "dict"
        assert s.prototype == "process(request: dict) -> dict"
        assert len(s.params) == 1
        assert s.params[0]["name"] == "request"
        assert s.params[0]["type"] == "dict"

    def test_typing_dict(self):
        """Dict from typing module should also be accepted."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict) -> Dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].params[0]["type"] == "Dict"
        assert schemas[0].return_type == "Dict"

    def test_parameterized_dict(self):
        """Dict[str, int] should be accepted as a Dict type."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, str]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].params[0]["type"] == "Dict[str, int]"
        assert schemas[0].return_type == "Dict[str, str]"

    def test_dict_with_nested_list(self):
        """Dict containing List values should be accepted."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, List[int]]) -> Dict[str, List[str]]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].params[0]["type"] == "Dict[str, List[int]]"
        assert schemas[0].return_type == "Dict[str, List[str]]"

    def test_dict_with_nested_dict(self):
        """Dict containing Dict values should be accepted."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, str]]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].params[0]["type"] == "Dict[str, Dict[str, int]]"

    def test_no_entry_func(self):
        source = textwrap.dedent("""\
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert schemas == []

    def test_no_docstring(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                return request
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
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_multiple_entry_functions(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(request: dict) -> dict:
                return {}

            @entry_func
            def multiply(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 2
        assert schemas[0].name == "add"
        assert schemas[1].name == "multiply"

    def test_prototype_shows_nested_types(self):
        """The prototype string should display full generic type annotations."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, List[int]]) -> Dict[str, int]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].prototype == (
            "process(request: Dict[str, List[int]]) -> Dict[str, int]"
        )


# ---------------------------------------------------------------------------
# extract_entry_functions – error cases
# ---------------------------------------------------------------------------


class TestExtractEntryFunctionsErrors:
    def test_missing_type_annotation_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request) -> dict:
                return request
        """)
        with pytest.raises(ValueError, match="missing a type annotation"):
            extract_entry_functions(source)

    def test_multiple_params_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict, extra: int) -> dict:
                return request
        """)
        with pytest.raises(ValueError, match="exactly one parameter"):
            extract_entry_functions(source)

    def test_zero_params_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process() -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="exactly one parameter.*got 0"):
            extract_entry_functions(source)

    def test_wrong_param_name_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(data: dict) -> dict:
                return data
        """)
        with pytest.raises(ValueError, match="named 'request', got 'data'"):
            extract_entry_functions(source)

    def test_non_dict_param_type_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: str) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="must be Dict type, got 'str'"):
            extract_entry_functions(source)

    def test_int_param_type_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: int) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="must be Dict type, got 'int'"):
            extract_entry_functions(source)

    def test_list_param_type_raises(self):
        source = textwrap.dedent("""\
            from typing import List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: List[int]) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="must be Dict type"):
            extract_entry_functions(source)

    def test_no_return_type_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict):
                pass
        """)
        with pytest.raises(ValueError, match="must have a Dict return type"):
            extract_entry_functions(source)

    def test_non_dict_return_type_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> int:
                return 0
        """)
        with pytest.raises(ValueError, match="must return Dict type, got 'int'"):
            extract_entry_functions(source)

    def test_str_return_type_raises(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> str:
                return ""
        """)
        with pytest.raises(ValueError, match="must return Dict type, got 'str'"):
            extract_entry_functions(source)

    def test_list_return_type_raises(self):
        source = textwrap.dedent("""\
            from typing import List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> List[int]:
                return []
        """)
        with pytest.raises(ValueError, match="must return Dict type"):
            extract_entry_functions(source)

    def test_any_in_param_type_raises(self):
        """Dict[str, Any] in request param should be rejected."""
        source = textwrap.dedent("""\
            from typing import Any, Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, Any]) -> Dict[str, int]:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)

    def test_any_in_return_type_raises(self):
        """Dict[str, Any] in return type should be rejected."""
        source = textwrap.dedent("""\
            from typing import Any, Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, Any]:
                return {}
        """)
        with pytest.raises(ValueError, match="Return type.*uses 'Any'.*not allowed"):
            extract_entry_functions(source)

    def test_any_nested_in_param_raises(self):
        """Dict[str, Dict[str, Any]] should also be rejected."""
        source = textwrap.dedent("""\
            from typing import Any, Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)

    def test_any_in_list_inside_dict_raises(self):
        """Dict[str, List[Any]] should be rejected."""
        source = textwrap.dedent("""\
            from typing import Any, Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, List[Any]]) -> Dict[str, int]:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)


# ---------------------------------------------------------------------------
# generate_openapi
# ---------------------------------------------------------------------------


class TestGenerateOpenapi:
    def test_basic_spec_bare_dict(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)

        assert spec["openapi"] == "3.0.0"
        assert spec["info"]["title"] == "Process Service"
        assert "/process" in spec["paths"]

        post = spec["paths"]["/process"]["post"]
        assert post["operationId"] == "process"
        assert post["summary"] == "Process a request"

        x_fn = post["x-function"]
        assert x_fn["language"] == "python"
        assert x_fn["prototype"] == "process(request: dict) -> dict"

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert req_schema == {"type": "object"}

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema == {"type": "object"}

    def test_custom_namespace_and_package(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas, namespace="testOrg", package="testPkg")
        x_fn = spec["paths"]["/process"]["post"]["x-function"]
        assert x_fn["namespace"] == "testOrg"
        assert x_fn["package"] == "testPkg"

    def test_empty_schemas_raises(self):
        with pytest.raises(ValueError, match="No @entry_func"):
            generate_openapi([])

    def test_400_response_present(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                return request
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        assert "400" in spec["paths"]["/process"]["post"]["responses"]

    def test_output_is_valid_yaml(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        yaml_str = yaml.dump(spec, default_flow_style=False, sort_keys=False)
        reloaded = yaml.safe_load(yaml_str)
        assert reloaded["openapi"] == "3.0.0"

    def test_multiple_functions_spec(self):
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def add(request: dict) -> dict:
                \"\"\"Add numbers.\"\"\"
                return {}

            @entry_func
            def multiply(request: dict) -> dict:
                \"\"\"Multiply numbers.\"\"\"
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        assert "/add" in spec["paths"]
        assert "/multiply" in spec["paths"]


# ---------------------------------------------------------------------------
# Nested type tests (Dict containing other types)
# ---------------------------------------------------------------------------


class TestNestedTypes:
    """Tests for Dict containing nested types like Dict[str, List[int]]."""

    def test_dict_str_int_request(self):
        """Dict[str, int] → object with additionalProperties integer."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, str]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {"type": "object", "additionalProperties": {"type": "integer"}}
        resp = spec["paths"]["/process"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert resp == {"type": "object", "additionalProperties": {"type": "string"}}

    def test_dict_nested_in_dict(self):
        """Dict[str, Dict[str, int]] → nested object with additionalProperties."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, Dict[str, int]]) -> Dict[str, int]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {
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
            def deep(request: Dict[str, Dict[str, Dict[str, float]]]) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/deep"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "number"},
                },
            },
        }

    def test_dict_of_list(self):
        """Dict[str, List[int]] → object with array additionalProperties."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, List[int]]) -> Dict[str, str]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }

    def test_dict_of_list_of_dict(self):
        """Dict[str, List[Dict[str, int]]] → object with array of objects."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, List[Dict[str, int]]]) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/process"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": {"type": "integer"},
                },
            },
        }

    def test_nested_return_type(self):
        """Return type Dict[str, List[int]] should be reflected in response."""
        source = textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def group_data(request: dict) -> Dict[str, List[int]]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        resp = spec["paths"]["/group_data"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert resp == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }

    def test_complex_end_to_end_yaml(self, tmp_path):
        """Full round-trip: nested Dict types → YAML file → reload and verify."""
        src = tmp_path / "complex.py"
        src.write_text(textwrap.dedent("""\
            from typing import Dict, List
            def entry_func(fn):
                return fn

            @entry_func
            def analyze(request: Dict[str, Dict[str, int]]) -> Dict[str, List[int]]:
                \"\"\"Analyze data with nested config.\"\"\"
                return {}
        """))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())

        req = spec["paths"]["/analyze"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req == {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {"type": "integer"},
            },
        }

        resp = spec["paths"]["/analyze"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert resp == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "integer"},
            },
        }


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
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """))
        assert main([str(src)]) == 0

    def test_output_file(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())
        assert spec["openapi"] == "3.0.0"
        assert "/process" in spec["paths"]

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
            def process(request) -> dict:
                return request
        """))
        with pytest.raises(ValueError, match="missing a type annotation"):
            main([str(src)])

    def test_multiple_params_raises(self, tmp_path):
        src = tmp_path / "bad.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict, extra: int) -> dict:
                return request
        """))
        with pytest.raises(ValueError, match="exactly one parameter"):
            main([str(src)])

    def test_non_dict_return_raises(self, tmp_path):
        src = tmp_path / "bad.py"
        src.write_text(textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> int:
                return 0
        """))
        with pytest.raises(ValueError, match="must return Dict type"):
            main([str(src)])


# ---------------------------------------------------------------------------
# Config file & resolve_config tests
# ---------------------------------------------------------------------------

SAMPLE_SOURCE = textwrap.dedent("""\
    def entry_func(fn):
        return fn

    @entry_func
    def add(request: dict) -> dict:
        return request
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
