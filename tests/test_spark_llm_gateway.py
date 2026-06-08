from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.llm_gateway import DefaultSparkLLMGateway, LLMGatewayCallError
from datacustomcode.llm_gateway.spark_default import (
    _STATUS_ERROR,
    _STATUS_SUCCESS,
    _build_underlying_gateway,
    _invoke_llm_gateway,
    _invoke_llm_gateway_as_struct,
)
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse


def _success_response(text: str = "ok") -> GenerateTextResponse:
    return GenerateTextResponse(
        status_code=200, data={"generation": {"generatedText": text}}
    )


def _error_response(
    status_code: int = 500, error_code: str = "INTERNAL_ERROR"
) -> GenerateTextResponse:
    return GenerateTextResponse(status_code=status_code, data={"errorCode": error_code})


class TestDefaultSparkLLMGatewayConstruction:
    """Construction wires an underlying ``LLMGateway``."""

    def test_uses_injected_llm_gateway_when_provided(self):
        injected = MagicMock()
        gateway = DefaultSparkLLMGateway(llm_gateway=injected)
        assert gateway._llm_gateway is injected

    @patch("datacustomcode.llm_gateway.spark_default._build_underlying_gateway")
    def test_falls_back_to_config_when_no_gateway_injected(self, mock_build):
        config_built = MagicMock()
        mock_build.return_value = config_built

        gateway = DefaultSparkLLMGateway()

        mock_build.assert_called_once_with()
        assert gateway._llm_gateway is config_built


class TestBuildUnderlyingGateway:
    """``_build_underlying_gateway`` resolves the config-defined ``LLMGateway``."""

    def test_returns_object_from_config(self):
        with patch(
            "datacustomcode.llm_gateway_config.llm_gateway_config"
        ) as mock_obj_config:
            mock_gateway = MagicMock()
            mock_obj_config.llm_gateway_config.to_object.return_value = mock_gateway

            assert _build_underlying_gateway() is mock_gateway
            mock_obj_config.llm_gateway_config.to_object.assert_called_once_with()

    def test_raises_when_config_missing(self):
        with patch(
            "datacustomcode.llm_gateway_config.llm_gateway_config"
        ) as mock_obj_config:
            mock_obj_config.llm_gateway_config = None
            with pytest.raises(RuntimeError, match="llm_gateway_config"):
                _build_underlying_gateway()


class TestDefaultSparkLLMGatewayGenerateText:

    def test_forwards_prompt_and_model(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("hello back")
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        result = gateway.llm_gateway_generate_text("hello", model_id="m1")

        assert result == "hello back"
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "hello"
        assert sent.model_name == "m1"

    def test_applies_default_model_when_omitted(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("ok")
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        gateway.llm_gateway_generate_text("just a prompt")

        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.model_name == "sfdc_ai__DefaultGPT4Omni"


class TestDefaultSparkLLMGatewayGenerateTextCol:

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_dict_values_built_into_struct_and_wrapped_in_udf(
        self, mock_struct, mock_udf
    ):
        sentinel_struct_col = MagicMock(name="struct_col")
        mock_struct.return_value = sentinel_struct_col
        sentinel_udf = MagicMock(name="udf")
        sentinel_applied = MagicMock(name="udf_applied")
        sentinel_udf.return_value = sentinel_applied
        mock_udf.return_value = sentinel_udf

        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("row-out")
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        name_col, city_col = MagicMock(name="name_col"), MagicMock(name="city_col")
        name_aliased, city_aliased = (
            MagicMock(name="name_aliased"),
            MagicMock(name="city_aliased"),
        )
        name_col.alias.return_value = name_aliased
        city_col.alias.return_value = city_aliased

        result = gateway.llm_gateway_generate_text_col(
            "Greet {name} from {city}.",
            {"name": name_col, "city": city_col},
            model_id="test-model",
        )

        name_col.alias.assert_called_once_with("name")
        city_col.alias.assert_called_once_with("city")
        mock_struct.assert_called_once_with(name_aliased, city_aliased)
        mock_udf.assert_called_once()
        sentinel_udf.assert_called_once_with(sentinel_struct_col)
        assert result is sentinel_applied

        udf_fn = mock_udf.call_args.args[0]
        row = MagicMock()
        row.asDict.return_value = {"name": "Ada", "city": "London"}
        out = udf_fn(row)

        assert out == {
            "status": _STATUS_SUCCESS,
            "response": "row-out",
            "error_code": None,
            "error_message": None,
        }
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "Greet Ada from London."
        assert sent.model_name == "test-model"

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_column_values_passed_through_without_struct(self, mock_struct, mock_udf):
        from pyspark.sql import Column

        existing_col = MagicMock(spec=Column)
        sentinel_udf = MagicMock(name="udf")
        sentinel_udf.return_value = MagicMock(name="udf_applied")
        mock_udf.return_value = sentinel_udf

        gateway = DefaultSparkLLMGateway(llm_gateway=MagicMock())

        gateway.llm_gateway_generate_text_col("Greet {name}", existing_col)

        mock_struct.assert_not_called()
        sentinel_udf.assert_called_once_with(existing_col)

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_udf_returns_error_struct_for_null_row(self, mock_struct, mock_udf):
        mock_struct.return_value = MagicMock()
        mock_udf.return_value = MagicMock()
        mock_inner = MagicMock()
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        gateway.llm_gateway_generate_text_col("template", {"placeholder": MagicMock()})

        udf_fn = mock_udf.call_args.args[0]
        out = udf_fn(None)
        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert "null" in out["error_message"].lower()
        mock_inner.generate_text.assert_not_called()

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_udf_returns_error_struct_on_http_error(self, mock_struct, mock_udf):
        """Per-row HTTP errors are returned as ``status="ERROR"`` structs so
        one bad row does not abort the Spark job."""
        mock_struct.return_value = MagicMock()
        mock_udf.return_value = MagicMock()
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        gateway.llm_gateway_generate_text_col("Greet {name}", {"name": MagicMock()})

        udf_fn = mock_udf.call_args.args[0]
        row = MagicMock()
        row.asDict.return_value = {"name": "Ada"}
        out = udf_fn(row)

        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert out["error_code"] == "UNAVAILABLE"
        assert out["error_message"] is not None


class TestInvokeLLMGateway:

    def test_returns_response_text(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("done")

        assert _invoke_llm_gateway(mock_inner, "prompt", "model") == "done"
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "prompt"
        assert sent.model_name == "model"

    def test_uses_default_model_when_none(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("ok")

        _invoke_llm_gateway(mock_inner, "prompt", None)
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.model_name == "sfdc_ai__DefaultGPT4Omni"

    def test_raises_llm_gateway_call_error_on_error_response(self):
        """``is_error`` responses surface as ``LLMGatewayCallError`` with the
        status code and error code attached for programmatic inspection."""
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )

        with pytest.raises(LLMGatewayCallError) as excinfo:
            _invoke_llm_gateway(mock_inner, "prompt", "model")

        assert excinfo.value.status == 503
        assert excinfo.value.error_code == "UNAVAILABLE"
        assert "503" in str(excinfo.value)
        assert "UNAVAILABLE" in str(excinfo.value)


class TestInvokeLLMGatewayAsStruct:
    """Non-raising variant of ``_invoke_llm_gateway`` used by the per-row UDF.
    Both SUCCESS and ERROR cases land in the same struct shape so callers can
    select fields uniformly."""

    def test_success_returns_success_struct(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("howdy")

        out = _invoke_llm_gateway_as_struct(mock_inner, "prompt", "model")

        assert out == {
            "status": _STATUS_SUCCESS,
            "response": "howdy",
            "error_code": None,
            "error_message": None,
        }

    def test_error_returns_error_struct_without_raising(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )

        out = _invoke_llm_gateway_as_struct(mock_inner, "prompt", "model")

        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert out["error_code"] == "UNAVAILABLE"
        assert out["error_message"] is not None

    def test_uses_default_model_when_none(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("ok")

        _invoke_llm_gateway_as_struct(mock_inner, "prompt", None)
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.model_name == "sfdc_ai__DefaultGPT4Omni"


class TestDefaultSparkLLMGatewayGenerateTextErrorHandling:
    """The scalar generate_text path raises when the underlying gateway errors."""

    def test_raises_on_error_response(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _error_response(
            status_code=429, error_code="RATE_LIMITED"
        )
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        with pytest.raises(LLMGatewayCallError) as excinfo:
            gateway.llm_gateway_generate_text("hello")

        assert excinfo.value.status == 429
        assert excinfo.value.error_code == "RATE_LIMITED"
