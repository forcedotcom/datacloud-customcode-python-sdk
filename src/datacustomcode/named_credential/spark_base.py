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
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from datacustomcode.mixin import UserExtendableNamedConfigMixin

if TYPE_CHECKING:
    from pyspark.sql import Column

    from datacustomcode.named_credential.types.http_request import HTTPRequest
    from datacustomcode.named_credential.types.http_response import HTTPResponse


class SparkNamedCredential(ABC, UserExtendableNamedConfigMixin):
    """Named Credential external callout for script (Spark) code.

    The callout is a one-shot request that runs on the driver. The endpoint and
    its authentication are resolved from the Named Credential referenced by
    ``request.url``.
    """

    CONFIG_NAME: str

    def __init__(self, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def request(
        self,
        request: HTTPRequest,
        body: Optional[str] = None,
    ) -> HTTPResponse:
        """Make an external callout through a Named Credential.

        Args:
            request: The callout request
            body: Optional request body. Set the
                ``Content-Type`` header yourself; the SDK does not assume one.

        Returns:
            The external service's response.
        """
        ...

    @abstractmethod
    def request_col(
        self,
        request: HTTPRequest,
        body: Optional["Column"] = None,
    ) -> "Column":
        """Build a Spark ``Column`` that makes one external callout per row.

        The endpoint, method, and headers are fixed for the call (taken from
        ``request``); only ``body`` varies per row. Use this instead of
        :meth:`request` when the callout runs across a DataFrame so each row is
        dispatched independently rather than one-shot on the driver.

        Args:
            request: The callout template
            body: Optional per-row ``Column`` holding the request body as a
                string, sent verbatim (or null for no body).

        Returns:
            A ``Column`` yielding a struct
            ``{status, response, error_code, error_message}``. ``response`` is
            itself a struct ``{status_code, body, headers}`` carrying the callout
            response. Select a field with ``[...]``, e.g.
            ``request_col(...)["response"]["status_code"]``. Returning a struct
            means a single failing row does not abort the Spark job — a transport
            failure sets ``status`` to ``ERROR`` with ``error_message``, while a
            non-2xx HTTP response stays ``SUCCESS`` with its code in
            ``response.status_code``.
        """
        ...
