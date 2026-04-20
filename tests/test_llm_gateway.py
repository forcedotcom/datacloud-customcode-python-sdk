from __future__ import annotations

from pydantic import ValidationError
import pytest

from datacustomcode.llm_gateway.base import LLMGateway
from datacustomcode.llm_gateway.default import DefaultLLMGateway
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest
from datacustomcode.llm_gateway.types.generate_text_request_builder import (
    GenerateTextRequestBuilder,
)
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse
from datacustomcode.llm_gateway.types.generate_text_response_builder import (
    GenerateTextResponseBuilder,
)


class TestGenerateTextRequest:
    """Test GenerateTextRequest model."""

    def test_version_defaults_to_v1(self):
        """Test version field defaults to v1."""
        request = GenerateTextRequest(model_name="gpt-4", prompt="Hello")
        assert request.version == "v1"

    def test_model_name_required(self):
        """Test model_name is required."""
        with pytest.raises(ValidationError) as exc_info:
            GenerateTextRequest(prompt="Hello")
        # Error message uses camelCase alias
        assert "modelName" in str(exc_info.value) or "model_name" in str(exc_info.value)

    def test_prompt_required(self):
        """Test prompt is required."""
        with pytest.raises(ValidationError) as exc_info:
            GenerateTextRequest(model_name="gpt-4")
        assert "prompt" in str(exc_info.value)

    def test_version_must_be_v1(self):
        """Test version must be literal 'v1'."""
        with pytest.raises(ValidationError) as exc_info:
            GenerateTextRequest(version="v2", model_name="gpt-4", prompt="Hello")
        assert "version" in str(exc_info.value)

    def test_camel_case_serialization(self):
        """Test serialization with camelCase aliases."""
        request = GenerateTextRequest(model_name="gpt-4", prompt="Hello")
        data = request.model_dump(by_alias=True)
        assert "modelName" in data
        assert data["modelName"] == "gpt-4"
        assert "model_name" not in data

    def test_accepts_camel_case_input(self):
        """Test model can accept camelCase field names."""
        request = GenerateTextRequest(modelName="gpt-4", prompt="Hello")
        assert request.model_name == "gpt-4"


class TestGenerateTextRequestBuilder:
    """Test GenerateTextRequestBuilder."""

    def test_builder_basic_usage(self):
        """Test basic builder pattern."""
        builder = GenerateTextRequestBuilder()
        request = builder.set_prompt("Hello").set_model("gpt-4").build()
        assert request.prompt == "Hello"
        assert request.model_name == "gpt-4"

    def test_builder_with_localization_dict(self):
        """Test builder with localization dictionary."""
        builder = GenerateTextRequestBuilder()
        localization = {"defaultLocale": "en-US", "timezone": "PST"}
        request = (
            builder.set_prompt("Hello")
            .set_model("gpt-4")
            .set_localization(localization=localization)
            .build()
        )
        assert request.localization == localization

    def test_builder_with_locale_string(self):
        """Test builder with simple locale string."""
        builder = GenerateTextRequestBuilder()
        request = (
            builder.set_prompt("Hello")
            .set_model("gpt-4")
            .set_localization(locale="en-US")
            .build()
        )
        # Verify the localization structure
        assert request.localization is not None
        assert request.localization["defaultLocale"] == "en-US"
        assert request.localization["inputLocales"] == [
            {"locale": "en-US", "probability": 1.0}
        ]
        assert request.localization["expectedLocales"] == ["en-US"]

    def test_builder_with_tags(self):
        """Test builder with tags."""
        builder = GenerateTextRequestBuilder()
        tags = {"user": "test", "session": "123"}
        request = builder.set_prompt("Hello").set_model("gpt-4").set_tags(tags).build()
        assert request.tags == tags

    def test_builder_validates_on_build(self):
        """Test builder validates request on build."""
        builder = GenerateTextRequestBuilder()
        with pytest.raises(ValidationError):
            builder.set_prompt("Hello").set_model("").build()

    def test_builder_localization_requires_argument(self):
        """Test set_localization requires either localization or locale."""
        builder = GenerateTextRequestBuilder()
        with pytest.raises(ValueError) as exc_info:
            builder.set_localization()
        assert "Must provide either localization or locale" in str(exc_info.value)


class TestGenerateTextResponse:
    """Test GenerateTextResponse model."""

    def test_response_defaults(self):
        """Test response field defaults."""
        response = GenerateTextResponse(status_code=200)
        assert response.version == "v1"
        assert response.data is None

    def test_is_success_property(self):
        """Test is_success property returns True for 200."""
        response = GenerateTextResponse(status_code=200)
        assert response.is_success is True
        assert response.is_error is False

    def test_is_success_false_for_non_200(self):
        """Test is_success property returns False for non-200."""
        response = GenerateTextResponse(status_code=400)
        assert response.is_success is False
        assert response.is_error is True

    def test_text_property_success(self):
        """Test text property extracts generated text on success."""
        response = GenerateTextResponse(
            status_code=200, data={"generation": {"generatedText": "Hello world"}}
        )
        assert response.text == "Hello world"

    def test_text_property_returns_empty_on_error(self):
        """Test text property returns empty string on error."""
        response = GenerateTextResponse(status_code=400, data={"error": "Bad request"})
        assert response.text == ""

    def test_text_property_handles_missing_data(self):
        """Test text property handles missing data gracefully."""
        response = GenerateTextResponse(status_code=200, data=None)
        assert response.text == ""

    def test_text_property_handles_missing_nested_fields(self):
        """Test text property handles missing nested fields."""
        response = GenerateTextResponse(status_code=200, data={"other": "data"})
        assert response.text == ""

    def test_error_code_property(self):
        """Test error_code property extracts error code on error."""
        response = GenerateTextResponse(
            status_code=400, data={"errorCode": "INVALID_REQUEST"}
        )
        assert response.error_code == "INVALID_REQUEST"

    def test_error_code_falls_back_to_status_code(self):
        """Test error_code falls back to status_code if no errorCode in data."""
        response = GenerateTextResponse(status_code=500, data={"message": "error"})
        assert response.error_code == "500"

    def test_error_code_returns_empty_on_success(self):
        """Test error_code returns empty string on success."""
        response = GenerateTextResponse(status_code=200)
        assert response.error_code == ""

    def test_status_code_validation(self):
        """Test status_code must be >= 0."""
        with pytest.raises(ValidationError) as exc_info:
            GenerateTextResponse(status_code=-1)
        assert "status_code" in str(exc_info.value)


class TestGenerateTextResponseBuilder:
    """Test GenerateTextResponseBuilder."""

    def test_builder_build_from_dict(self):
        """Test building response from dictionary."""
        response_dict = {
            "version": "v1",
            "status_code": 200,
            "data": {"generation": {"generatedText": "Hello world"}},
        }
        response = GenerateTextResponseBuilder.build(response_dict)
        assert isinstance(response, GenerateTextResponse)
        assert response.status_code == 200
        assert response.text == "Hello world"

    def test_builder_validates_dict(self):
        """Test builder validates the dictionary."""
        invalid_dict = {"version": "v1"}  # Missing required status_code
        with pytest.raises(ValidationError):
            GenerateTextResponseBuilder.build(invalid_dict)

    def test_builder_with_minimal_dict(self):
        """Test builder with minimal required fields."""
        response_dict = {"status_code": 200}
        response = GenerateTextResponseBuilder.build(response_dict)
        assert response.status_code == 200
        assert response.version == "v1"  # Default value


class TestDefaultLLMGateway:
    """Test DefaultLLMGateway implementation."""

    def test_default_gateway_is_llm_gateway(self):
        """Test DefaultLLMGateway inherits from LLMGateway."""
        gateway = DefaultLLMGateway()
        assert isinstance(gateway, LLMGateway)

    def test_generate_text_returns_response(self):
        """Test generate_text returns GenerateTextResponse."""
        gateway = DefaultLLMGateway()
        request = GenerateTextRequest(model_name="gpt-4", prompt="Hello")
        response = gateway.generate_text(request)
        assert isinstance(response, GenerateTextResponse)

    def test_generate_text_success_response(self):
        """Test generate_text returns successful response."""
        gateway = DefaultLLMGateway()
        request = GenerateTextRequest(model_name="gpt-4", prompt="Hello")
        response = gateway.generate_text(request)
        assert response.is_success is True
        assert response.status_code == 200
        assert len(response.text) > 0
