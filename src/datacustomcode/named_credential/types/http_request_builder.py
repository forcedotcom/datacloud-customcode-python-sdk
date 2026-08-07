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

from typing import Dict, Union

from datacustomcode.named_credential.types.http_method import HTTPMethod
from datacustomcode.named_credential.types.http_request import (
    RESPONSE_TIMEOUT_HEADER,
    HTTPRequest,
)


class HTTPRequestBuilder:
    def __init__(self) -> None:
        self._url = ""
        self._method: Union[str, HTTPMethod] = HTTPMethod.GET
        self._headers: Dict[str, str] = {}

    def set_url(self, url: str) -> "HTTPRequestBuilder":
        """Set the symbolic Named Credential reference.

        Args:
            url: e.g. 'callout:<NamedCredential>/<path>'
        """
        self._url = url
        return self

    def set_method(self, method: Union[str, HTTPMethod]) -> "HTTPRequestBuilder":
        """Set the HTTP method.

        Accepts this module's ``HTTPMethod``, ``http.HTTPMethod`` (Python 3.11+),
        or a plain string such as ``"GET"``.
        """
        self._method = method
        return self

    def set_headers(self, headers: Dict[str, str]) -> "HTTPRequestBuilder":
        self._headers = headers
        return self

    def set_response_timeout_seconds(self, seconds: int) -> "HTTPRequestBuilder":
        """Override the callout response timeout for this request.

        Sets the ``ctx-callout-response-timeout-seconds`` control header, which
        byoc-proxy uses to override its default response timeout (subject to the
        proxy's own validation and server-side maximum). On the local
        (``datacustomcode run``) path it sets the outbound HTTP timeout. The
        header is never forwarded to the external service.

        Args:
            seconds: The response timeout in seconds; must be a positive integer.
        """
        if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds <= 0:
            raise ValueError(
                f"response_timeout_seconds must be a positive integer, got {seconds!r}"
            )
        self._headers[RESPONSE_TIMEOUT_HEADER] = str(seconds)
        return self

    def build(self) -> HTTPRequest:
        return HTTPRequest(
            url=self._url,
            method=self._method,  # type: ignore[arg-type]
            headers=self._headers,
        )
