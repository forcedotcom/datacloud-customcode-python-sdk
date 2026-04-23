# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datacustomcode.llm_gateway.base import LLMGateway
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse
from datacustomcode.llm_gateway_config import LLMGatewayObjectConfig


class TestCustomLLMGatewayImplementation:
    def test_custom_implementation_is_discoverable(self):
        class CustomLLMGateway(LLMGateway):
            CONFIG_NAME = "CustomLLMGateway"

            def __init__(self, custom_param: str = "default", **kwargs):
                super().__init__(**kwargs)
                self.custom_param = custom_param

            def generate_text(
                self, request: GenerateTextRequest
            ) -> GenerateTextResponse:
                return GenerateTextResponse(
                    version="v1",
                    status_code=200,
                    data={"generation": {"generatedText": "Custom response"}},
                )

        available_names = LLMGateway.available_config_names()
        assert "CustomLLMGateway" in available_names

        cls = LLMGateway.subclass_from_config_name("CustomLLMGateway")
        assert cls == CustomLLMGateway

        # Verify we can create via config
        llm_config = LLMGatewayObjectConfig(
            type_config_name="CustomLLMGateway",
            options={"custom_param": "my_value"},
        )
        instance = llm_config.to_object()
        assert isinstance(instance, CustomLLMGateway)
        assert instance.custom_param == "my_value"

        request = GenerateTextRequest(model_name="test-model", prompt="Hello")
        response = instance.generate_text(request)
        assert response.is_success is True
        assert response.data["generation"]["generatedText"] == "Custom response"
