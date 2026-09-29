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
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from datacustomcode.mixin import UserExtendableNamedConfigMixin

if TYPE_CHECKING:
    from datacustomcode.named_credential.types.http_request import HTTPRequest
    from datacustomcode.named_credential.types.http_response import HTTPResponse


class NamedCredential(ABC, UserExtendableNamedConfigMixin):
    CONFIG_NAME: str

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def request(
        self,
        request: HTTPRequest,
        body: Optional[str] = None,
    ) -> HTTPResponse:
        """Make an external callout through a Named Credential.

        The endpoint and its authentication are resolved server-side from the
        Named Credential referenced by ``request.url``; the function never sees
        the external credential.

        Args:
            request: The callout request
            body: Optional request body. Set the
                ``Content-Type`` header to match the format; the SDK
                does not assume or inject one.

        Returns:
            The external service's response.
        """
        ...
