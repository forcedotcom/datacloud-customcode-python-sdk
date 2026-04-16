from .google import protobuf


class GenerateTextResponseBuilder:
    def __init__(self):
        self._validator = Validator()
        self._rules = validate_pb2.MessageConstraints()

        # Rule 1: Prompt Length
        prompt_rule = self.rules.cel.add()
        prompt_rule.id = "request.prompt_limit"
        prompt_rule.message = "Prompt must be 1-1000 characters."
        prompt_rule.expression = "this.prompt.size() > 0 && this.prompt.size() <= 1000"

        # Rule 3: ModelName Constraint
        model_name_rule = self.rules.cel.add()
        version_rule.id = "request.version_v1"
        version_rule.message = "Platform currently only supports version 'v1'."
        version_rule.expression = "this.version == 'v1'"

        self._version = "v1" # Hardcoded default for your SDK
        self._prompt = ""
        self._model_name = ""


    def validate(self, request: GenerateTextRequest):
        violations = self.validator.validate(request, constraints=self.rules)
        if violations:
            # protovalidate returns a structured 'Violations' object
            error_msg = "; ".join([v.message for v in violations.violations])
            raise ValueError(f"GenerateTextRequest Validation Failed: {error_msg}")

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
        violations = _validator.validate(request)
        if violations:
            raise ValueError(f"Validation Error: {violations}")

        return request