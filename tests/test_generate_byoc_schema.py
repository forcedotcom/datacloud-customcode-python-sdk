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
            from typing import Dict
            from datacustomcode.entry_func import entry_func

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_schema_extraction_with_top_level_import(self):
        """Source using top-level SDK import should be extractable."""
        source = textwrap.dedent("""\
            from typing import Dict
            from datacustomcode import entry_func

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_empty_lines_between_decorator_and_function(self):
        """Multiple blank lines between @entry_func and def should still work."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn


            @entry_func


            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"
        assert schemas[0].prototype == "process(request: Dict[str, int]) -> Dict[str, int]"


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
    def test_bare_dict_without_decorator_raises(self):
        """Bare dict without @requestSchema/@responseSchema must error."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> dict:
                \"\"\"Process a request.\"\"\"
                return request
        """)
        with pytest.raises(ValueError, match="bare dict without @requestSchema"):
            extract_entry_functions(source)

    def test_bare_typing_dict_without_decorator_raises(self):
        """Bare Dict (from typing) without decorators must error."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict) -> Dict:
                return request
        """)
        with pytest.raises(ValueError, match="bare dict without @requestSchema"):
            extract_entry_functions(source)

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
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].docstring is None

    def test_ignores_non_decorated_functions(self):
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            def helper(x: int) -> int:
                return x + 1

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert schemas[0].name == "process"

    def test_multiple_entry_functions(self):
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def add(request: Dict[str, int]) -> Dict[str, int]:
                return {}

            @entry_func
            def multiply(request: Dict[str, int]) -> Dict[str, int]:
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

    def test_bare_dict_request_without_requestSchema_raises(self):
        """Bare dict request with parameterized return should error."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: dict) -> Dict[str, int]:
                return {}
        """)
        with pytest.raises(ValueError, match="bare dict without @requestSchema"):
            extract_entry_functions(source)

    def test_bare_dict_return_without_responseSchema_raises(self):
        """Parameterized request with bare dict return should error."""
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="bare dict without @responseSchema"):
            extract_entry_functions(source)

    def test_bare_dict_both_only_requestSchema_raises(self):
        """Bare dict on both sides with only @requestSchema should error on return."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec

            class Req:
                a: int

            @entry_func
            @requestSchema(Req)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="bare dict without @responseSchema"):
            extract_entry_functions(source)

    def test_bare_dict_both_only_responseSchema_raises(self):
        """Bare dict on both sides with only @responseSchema should error on request."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Resp:
                result: int

            @entry_func
            @responseSchema(Resp)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="bare dict without @requestSchema"):
            extract_entry_functions(source)


# ---------------------------------------------------------------------------
# @requestSchema / @responseSchema decorator tests
# ---------------------------------------------------------------------------


class TestSchemaDecorators:
    """Tests for @requestSchema and @responseSchema decorator support."""

    def test_both_decorators_with_typed_dict(self):
        """Both @requestSchema and @responseSchema produce named properties."""
        source = textwrap.dedent("""\
            from typing import TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                a: int
                b: int

            class Resp(TypedDict):
                result: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def add(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        s = schemas[0]
        assert s.name == "add"
        assert s.input_schema is not None
        assert s.output_schema is not None
        assert len(s.input_schema.fields) == 2
        assert s.input_schema.fields[0][0] == "a"
        assert s.input_schema.fields[1][0] == "b"
        assert len(s.output_schema.fields) == 1
        assert s.output_schema.fields[0][0] == "result"

    def test_requestSchema_only_with_parameterized_return(self):
        """@requestSchema with inline parameterized return type."""
        source = textwrap.dedent("""\
            from typing import Dict, List, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                values: List[int]

            @entry_func
            @requestSchema(Req)
            def process(request: dict) -> Dict[str, int]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].input_schema is not None
        assert schemas[0].output_schema is None
        assert schemas[0].return_type == "Dict[str, int]"

    def test_responseSchema_only_with_parameterized_request(self):
        """@responseSchema with inline parameterized request type."""
        source = textwrap.dedent("""\
            from typing import Dict, TypedDict
            def entry_func(fn):
                return fn
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Resp(TypedDict):
                result: int

            @entry_func
            @responseSchema(Resp)
            def process(request: Dict[str, int]) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert schemas[0].input_schema is None
        assert schemas[0].output_schema is not None

    def test_schema_class_not_found_raises(self):
        """Referencing a class that doesn't exist should error."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec

            @entry_func
            @requestSchema(MissingClass)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="not found in the source"):
            extract_entry_functions(source)

    def test_schema_class_no_fields_raises(self):
        """A class with no annotated fields should error."""
        source = textwrap.dedent("""\
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Empty:
                pass

            class Resp:
                result: int

            @entry_func
            @requestSchema(Empty)
            @responseSchema(Resp)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="has no typed fields"):
            extract_entry_functions(source)

    def test_any_in_requestSchema_class_raises(self):
        """Any in @requestSchema class field should be rejected."""
        source = textwrap.dedent("""\
            from typing import Any
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req:
                data: Any

            class Resp:
                result: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)

    def test_any_in_responseSchema_class_raises(self):
        """Any in @responseSchema class field should be rejected."""
        source = textwrap.dedent("""\
            from typing import Any
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req:
                data: int

            class Resp:
                result: Any

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)

    def test_nested_types_in_schema_class(self):
        """Schema class fields with nested types should work."""
        source = textwrap.dedent("""\
            from typing import Dict, List, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                filters: Dict[str, List[str]]
                limit: int

            class Resp(TypedDict):
                rows: List[Dict[str, int]]

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def query(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        assert len(schemas) == 1
        assert len(schemas[0].input_schema.fields) == 2
        assert len(schemas[0].output_schema.fields) == 1

    def test_optional_field_not_required(self):
        """Optional[X] fields should appear in properties but not required."""
        source = textwrap.dedent("""\
            from typing import Optional, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                name: str
                greeting: Optional[str]

            class Resp(TypedDict):
                message: str

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def greet(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        post = spec["paths"]["/greet"]["post"]

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert "name" in req_schema["properties"]
        assert "greeting" in req_schema["properties"]
        assert req_schema["properties"]["name"] == {"type": "string"}
        assert req_schema["properties"]["greeting"] == {"type": "string"}
        assert req_schema["required"] == ["name"]

    def test_all_optional_fields_no_required_key(self):
        """When all fields are Optional, 'required' key should be absent."""
        source = textwrap.dedent("""\
            from typing import Optional, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                a: Optional[int]
                b: Optional[int]

            class Resp(TypedDict):
                result: Optional[int]

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def add(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        post = spec["paths"]["/add"]["post"]

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert "a" in req_schema["properties"]
        assert "b" in req_schema["properties"]
        assert "required" not in req_schema

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert "result" in resp_schema["properties"]
        assert "required" not in resp_schema

    def test_optional_nested_type(self):
        """Optional[Dict[str, int]] should unwrap to the nested type."""
        source = textwrap.dedent("""\
            from typing import Dict, List, Optional, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                dataset: str
                filters: Optional[Dict[str, List[str]]]
                limit: Optional[int]

            class Resp(TypedDict):
                rows: List[int]

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def analyze(request: dict) -> dict:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        req = spec["paths"]["/analyze"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert req["required"] == ["dataset"]
        assert req["properties"]["filters"] == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "string"},
            },
        }
        assert req["properties"]["limit"] == {"type": "integer"}

    def test_optional_any_still_rejected(self):
        """Optional[Any] should still be rejected."""
        source = textwrap.dedent("""\
            from typing import Any, Optional
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req:
                data: Optional[Any]

            class Resp:
                result: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def process(request: dict) -> dict:
                return {}
        """)
        with pytest.raises(ValueError, match="uses 'Any'.*not allowed"):
            extract_entry_functions(source)


# ---------------------------------------------------------------------------
# generate_openapi
# ---------------------------------------------------------------------------


class TestGenerateOpenapi:
    def test_basic_spec_parameterized_dict(self):
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
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
        assert x_fn["prototype"] == "process(request: Dict[str, int]) -> Dict[str, int]"
        assert "requestSchema" not in x_fn
        assert "responseSchema" not in x_fn

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert req_schema == {"type": "object", "additionalProperties": {"type": "integer"}}

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema == {"type": "object", "additionalProperties": {"type": "integer"}}

    def test_spec_with_schema_decorators(self):
        """@requestSchema/@responseSchema should produce named properties."""
        source = textwrap.dedent("""\
            from typing import TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                a: int
                b: int

            class Resp(TypedDict):
                result: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def add(request: dict) -> dict:
                \"\"\"Add two numbers.\"\"\"
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        post = spec["paths"]["/add"]["post"]

        x_fn = post["x-function"]
        assert x_fn["requestSchema"] == "dict(a: int, b: int)"
        assert x_fn["responseSchema"] == "dict(result: int)"

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert req_schema == {
            "type": "object",
            "properties": {
                "a": {"type": "integer"},
                "b": {"type": "integer"},
            },
            "required": ["a", "b"],
        }

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema == {
            "type": "object",
            "properties": {
                "result": {"type": "integer"},
            },
            "required": ["result"],
        }

    def test_spec_with_nested_schema_decorators(self):
        """Schema decorator classes with nested types should map correctly."""
        source = textwrap.dedent("""\
            from typing import Dict, List, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                filters: Dict[str, List[str]]
                limit: int

            class Resp(TypedDict):
                rows: List[Dict[str, int]]
                total: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def analyze(request: dict) -> dict:
                \"\"\"Analyze data.\"\"\"
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        post = spec["paths"]["/analyze"]["post"]

        x_fn = post["x-function"]
        assert x_fn["requestSchema"] == "dict(filters: Dict[str, List[str]], limit: int)"
        assert x_fn["responseSchema"] == "dict(rows: List[Dict[str, int]], total: int)"

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert req_schema == {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "limit": {"type": "integer"},
            },
            "required": ["filters", "limit"],
        }

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema == {
            "type": "object",
            "properties": {
                "rows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": {"type": "integer"},
                    },
                },
                "total": {"type": "integer"},
            },
            "required": ["rows", "total"],
        }

    def test_mixed_requestSchema_with_inline_return(self):
        """@requestSchema + parameterized return type should both work."""
        source = textwrap.dedent("""\
            from typing import Dict, List, TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                values: List[int]
                scale: float

            @entry_func
            @requestSchema(Req)
            def transform(request: dict) -> Dict[str, List[float]]:
                return {}
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        post = spec["paths"]["/transform"]["post"]

        x_fn = post["x-function"]
        assert x_fn["requestSchema"] == "dict(values: List[int], scale: float)"
        assert "responseSchema" not in x_fn

        req_schema = post["requestBody"]["content"]["application/json"]["schema"]
        assert req_schema["properties"]["values"] == {
            "type": "array",
            "items": {"type": "integer"},
        }
        assert req_schema["properties"]["scale"] == {"type": "number"}

        resp_schema = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp_schema == {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "number"},
            },
        }

    def test_custom_namespace_and_package(self):
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
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
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                return request
        """)
        schemas = extract_entry_functions(source)
        spec = generate_openapi(schemas)
        assert "400" in spec["paths"]["/process"]["post"]["responses"]

    def test_output_is_valid_yaml(self):
        source = textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
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
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def add(request: Dict[str, int]) -> Dict[str, int]:
                \"\"\"Add numbers.\"\"\"
                return {}

            @entry_func
            def multiply(request: Dict[str, int]) -> Dict[str, int]:
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
            def deep(request: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, int]:
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
            def process(request: Dict[str, List[Dict[str, int]]]) -> Dict[str, int]:
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
            def group_data(request: Dict[str, int]) -> Dict[str, List[int]]:
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

    def test_schema_decorator_end_to_end_yaml(self, tmp_path):
        """Full round-trip: @requestSchema/@responseSchema → YAML → verify properties."""
        src = tmp_path / "typed.py"
        src.write_text(textwrap.dedent("""\
            from typing import TypedDict
            def entry_func(fn):
                return fn
            def requestSchema(cls):
                def dec(fn): return fn
                return dec
            def responseSchema(cls):
                def dec(fn): return fn
                return dec

            class Req(TypedDict):
                a: int
                b: int

            class Resp(TypedDict):
                result: int

            @entry_func
            @requestSchema(Req)
            @responseSchema(Resp)
            def add(request: dict) -> dict:
                \"\"\"Add two numbers.\"\"\"
                return {}
        """))
        out = tmp_path / "out.yaml"
        assert main([str(src), "-o", str(out)]) == 0
        spec = yaml.safe_load(out.read_text())

        post = spec["paths"]["/add"]["post"]
        x_fn = post["x-function"]
        assert x_fn["requestSchema"] == "dict(a: int, b: int)"
        assert x_fn["responseSchema"] == "dict(result: int)"

        req = post["requestBody"]["content"]["application/json"]["schema"]
        assert req["properties"]["a"] == {"type": "integer"}
        assert req["properties"]["b"] == {"type": "integer"}
        assert set(req["required"]) == {"a", "b"}

        resp = post["responses"]["200"]["content"]["application/json"]["schema"]
        assert resp["properties"]["result"] == {"type": "integer"}
        assert resp["required"] == ["result"]


# ---------------------------------------------------------------------------
# CLI (main)
# ---------------------------------------------------------------------------


class TestMain:
    def test_stdout(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
                \"\"\"Process a request.\"\"\"
                return request
        """))
        assert main([str(src)]) == 0

    def test_output_file(self, tmp_path):
        src = tmp_path / "sample.py"
        src.write_text(textwrap.dedent("""\
            from typing import Dict
            def entry_func(fn):
                return fn

            @entry_func
            def process(request: Dict[str, int]) -> Dict[str, int]:
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
    from typing import Dict
    def entry_func(fn):
        return fn

    @entry_func
    def add(request: Dict[str, int]) -> Dict[str, int]:
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
