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

from typing import (
    Any,
    Dict,
    Optional,
)

from loguru import logger
import requests

from datacustomcode.einstein_platform_client import EinsteinPlatformClient
from datacustomcode.llm_gateway.base import LLMGateway
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse


class DefaultLLMGateway(EinsteinPlatformClient, LLMGateway):
    CONFIG_NAME = "DefaultLLMGateway"

    def __init__(
        self,
        credentials_profile: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
        **kwargs,
    ):
        EinsteinPlatformClient.__init__(
            self, credentials_profile=credentials_profile, sf_cli_org=sf_cli_org
        )
        LLMGateway.__init__(self, **kwargs)

    def generate_text(self, request: GenerateTextRequest) -> GenerateTextResponse:
        api_url = (
            f"{self.EINSTEIN_PLATFORM_URL}/models/{request.model_name}/generations"
        )

        payload: Dict[str, Any] = {"prompt": request.prompt}

        if request.localization:
            payload["localization"] = request.localization
        if request.tags:
            payload["tags"] = request.tags

        logger.debug(f"Making Generate text request: {api_url}")
        try:
            response = requests.post(
                api_url, json=payload, headers=self.get_headers(), timeout=180
            )
            if not response.ok and not response.text:
                error_msg = (
                    f"Generate text request failed: {api_url} - "
                    f"{response.status_code} {response.reason}"
                )
                logger.error(error_msg)
        except requests.exceptions.RequestException as e:
            logger.error(f"Generate text request failed: {api_url} {e}")
            raise RuntimeError(f"Generate text request failed: {e}") from e

        return GenerateTextResponse(
            status_code=response.status_code, data=self.parse_response(response)
        )
