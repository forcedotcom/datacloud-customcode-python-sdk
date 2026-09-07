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
"""
Inject external credential auth into an outgoing request.
"""

from __future__ import annotations

import base64
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
)

from requests.auth import AuthBase

from datacustomcode.named_credential.direct.credentials import AuthType

if TYPE_CHECKING:
    from requests.models import PreparedRequest


class DynamicAuthHandler(AuthBase):
    def __init__(self, cred_config: Dict[str, Any]) -> None:
        self.config = cred_config
        self.auth_type = cred_config.get("auth_type")

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.auth_type == AuthType.BASIC.value:
            user = self.config.get("username", "")
            pwd = self.config.get("password", "")
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            request.headers["Authorization"] = f"Basic {token}"

        elif self.auth_type == AuthType.CUSTOM.value:
            for name, value in self.config.get("custom_headers", {}).items():
                request.headers[name] = value

        elif self.auth_type in (AuthType.OAUTH.value, AuthType.JWT.value):
            bearer = self.config.get("access_token") or self.config.get("token")
            if not bearer:
                raise ValueError(
                    f"'{self.auth_type}' auth requires an 'access_token' or 'token'."
                )
            request.headers["Authorization"] = f"Bearer {bearer}"

        else:
            raise ValueError(f"Unsupported auth_type '{self.auth_type}'.")

        return request
