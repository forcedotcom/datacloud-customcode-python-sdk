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
    Dict,
    Optional,
    Union,
)

from datacustomcode.named_credential.types.http_method import HTTPMethod
from datacustomcode.named_credential.types.http_request import HTTPRequest

# Control header carrying the per-request callout response timeout (seconds).
CALLOUT_RESPONSE_TIMEOUT_HEADER = "ctx-callout-response-timeout-seconds"


class HTTPRequestBuilder:
    def __init__(self) -> None:
        self._url = ""
        self._method: Union[str, HTTPMethod] = HTTPMethod.GET
        self._headers: Dict[str, str] = {}
        self._response_timeout_seconds: Optional[int] = None

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
        """Set the HTTP headers.

        Args:
            headers: a dictionary of HTTP headers
        """
        self._headers = headers
        return self

    def set_response_timeout_seconds(self, seconds: int) -> "HTTPRequestBuilder":
        """Set the per-request callout response timeout, in seconds.

        The ``ctx-callout-response-timeout-seconds`` control header allows to
        configure the timeout for the callout response. This is optional.
        If not set, the platform will use the default timeout.
        """
        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds <= 0:
            raise ValueError(
                "response timeout must be a positive integer number of seconds, "
                f"got {seconds!r}."
            )
        self._response_timeout_seconds = seconds
        return self

    def build(self) -> HTTPRequest:
        headers = dict(self._headers)
        if self._response_timeout_seconds is not None:
            headers[CALLOUT_RESPONSE_TIMEOUT_HEADER] = str(
                self._response_timeout_seconds
            )
        return HTTPRequest(
            url=self._url,
            method=self._method,  # type: ignore[arg-type]
            headers=headers,
        )
