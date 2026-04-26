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

from datacustomcode.token_provider import (
    CredentialsTokenProvider,
    SFCLITokenProvider,
    TokenProvider,
)


class EinsteinPlatformClient:
    EINSTEIN_PLATFORM_URL = "https://api.salesforce.com/einstein/platform/v1"

    def __init__(
        self,
        credentials_profile: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
    ):
        if sf_cli_org:
            self._token_provider: TokenProvider = SFCLITokenProvider(sf_cli_org)
            logger.debug(f"Using SF CLI token provider for org: {sf_cli_org}")
        else:
            profile = credentials_profile or "default"
            self._token_provider = CredentialsTokenProvider(profile)
            logger.debug(f"Using credentials token provider with profile: {profile}")
        self.token_response = None

    def get_headers(self):
        if self.token_response is None:
            self.token_response = self._token_provider.get_token()

        return {
            "Authorization": f"Bearer {self.token_response.access_token}",
            "Content-Type": "application/json",
            "x-sfdc-app-context": "EinsteinGPT",
            "x-client-feature-id": "ai-platform-models-connected-app",
        }

    def parse_response(self, response):
        response_data: Dict[str, Any] = {}
        if response.content:
            try:
                response_data = response.json()
            except ValueError:
                logger.warning("Failed to parse response as JSON")
                response_data = {"raw_response": response.text}
        return response_data
