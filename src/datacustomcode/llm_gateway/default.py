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

from typing import Any, Dict

from datacustomcode.einstein_platform_client import EinsteinPlatformClient
from datacustomcode.llm_gateway.base import LLMGateway
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse
from datacustomcode.llm_gateway.types.generate_text_response_builder import (
    GenerateTextResponseBuilder,
)


class DefaultLLMGateway(EinsteinPlatformClient, LLMGateway):
    CONFIG_NAME = "DefaultLLMGateway"

    def generate_text(self, request: GenerateTextRequest) -> GenerateTextResponse:
        api_url = (
            f"{self.EINSTEIN_PLATFORM_MODELS_URL}/{request.model_name}/generations"
        )

        payload: Dict[str, Any] = {"prompt": request.prompt}

        if request.localization:
            payload["localization"] = request.localization
        if request.tags:
            payload["tags"] = request.tags

        response = self.make_post_request(api_url, payload)
        response_dict = {
            "status_code": response.status_code,
            "data": self.parse_response(response),
        }
        return GenerateTextResponseBuilder.build(response_dict)
