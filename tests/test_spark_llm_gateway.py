from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.llm_gateway import DefaultSparkLLMGateway
from datacustomcode.llm_gateway.spark_default import (
    _build_underlying_gateway,
    _invoke_llm_gateway,
)
from datacustomcode.llm_gateway.types.generate_text_response import (
    GenerateTextResponse,
)


def _success_response(text: str = "ok") -> GenerateTextResponse:
    return GenerateTextResponse(
        status_code=200, data={"generation": {"generatedText": text}}
    )


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

    def test_forwards_prompt_model_and_max_tokens(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("hello back")
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        result = gateway.llm_gateway_generate_text(
            "hello", model_id="m1", max_tokens=42
        )

        assert result == "hello back"
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "hello"
        assert sent.model_name == "m1"
        assert sent.max_tokens == 42

    def test_applies_defaults_when_model_and_tokens_omitted(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("ok")
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        gateway.llm_gateway_generate_text("just a prompt")

        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.model_name == "sfdc_ai__DefaultGPT4Omni"
        assert sent.max_tokens == 200


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
            max_tokens=5,
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

        assert out == "row-out"
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "Greet Ada from London."
        assert sent.model_name == "test-model"
        assert sent.max_tokens == 5

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
    def test_udf_returns_empty_for_null_row(self, mock_struct, mock_udf):
        mock_struct.return_value = MagicMock()
        mock_udf.return_value = MagicMock()
        mock_inner = MagicMock()
        gateway = DefaultSparkLLMGateway(llm_gateway=mock_inner)

        gateway.llm_gateway_generate_text_col("template", {"placeholder": MagicMock()})

        udf_fn = mock_udf.call_args.args[0]
        assert udf_fn(None) == ""
        mock_inner.generate_text.assert_not_called()


class TestInvokeLLMGateway:

    def test_returns_response_text(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("done")

        assert _invoke_llm_gateway(mock_inner, "prompt", "model", 7) == "done"
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.prompt == "prompt"
        assert sent.model_name == "model"
        assert sent.max_tokens == 7

    def test_uses_defaults_when_model_and_tokens_none(self):
        mock_inner = MagicMock()
        mock_inner.generate_text.return_value = _success_response("ok")

        _invoke_llm_gateway(mock_inner, "prompt", None, None)
        sent = mock_inner.generate_text.call_args.args[0]
        assert sent.model_name == "sfdc_ai__DefaultGPT4Omni"
        assert sent.max_tokens == 200
