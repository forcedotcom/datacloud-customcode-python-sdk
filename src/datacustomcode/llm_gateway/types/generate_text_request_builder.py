from datacustomcode.validator.base import Validator
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest


class GenerateTextRequestValidator:
    @staticmethod
    def create_validator() -> Validator:
        """Create a validator with all CEL rules for GenerateTextRequest"""
        validator = Validator()

        # Rule 1: version == "v1" (CEL: const)
        validator.add_rule(
            id="request.version_v1",
            message="Platform currently only supports version 'v1'",
            expression=lambda this: this.version == "v1"
        )

        # Rule 2: modelName.size() >= 1 (CEL: min_len)
        validator.add_rule(
            id="request.model_name_required",
            message="modelName must not be empty (min_len: 1)",
            expression=lambda this: len(this.model_name) >= 1
        )

        return validator

class GenerateTextRequestBuilder:
    def __init__(self):
        self._validator = GenerateTextRequestValidator.create_validator()
        self._version = "v1" # Hardcoded default for your SDK
        self._prompt = ""
        self._model_name = ""


    def set_prompt(self, prompt: str):
        self._prompt = prompt
        return self

    def set_model(self, model_name: str):
        self._model_name = model_name
        return self

    def build(self) -> GenerateTextRequest:

        request = GenerateTextRequest(
            version=self._version,
            prompt=self._prompt,
            model_name=self._model_name
        )

        # 2. Run the Protovalidate check
        # This reads the 'max_len: 1000' rule from the .proto metadata
        violations = self._validator.validate(request)
        if violations:
            raise ValueError(f"Validation Error: {violations}")

        return request