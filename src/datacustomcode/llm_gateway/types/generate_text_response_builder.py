from typing import Dict, Any
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse


class GenerateTextResponseBuilder:
    def __init__(self):
        self._version = "v1" # Hardcoded default for your SDK
        self._status_code = None
        self._data = None

    def set_status_code(self, status_code: int):
        self._status_code = status_code
        return self

    def set_data(self, data: dict):
        self._data = data
        return self

    @staticmethod
    def build(response_dict: Dict[str, Any]) -> GenerateTextResponse:
        return GenerateTextResponse.model_validate(response_dict)