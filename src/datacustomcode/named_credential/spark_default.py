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

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

from datacustomcode.named_credential.spark_base import SparkNamedCredential

if TYPE_CHECKING:
    from pyspark.sql import Column

    from datacustomcode.named_credential.base import NamedCredential
    from datacustomcode.named_credential.types.http_request import HTTPRequest
    from datacustomcode.named_credential.types.http_response import HTTPResponse


_STATUS_SUCCESS = "SUCCESS"
_STATUS_ERROR = "ERROR"


def _build_underlying_named_credential() -> "NamedCredential":
    """Build the callout object from the configured ``named_credential_config``.

    Raises ``RuntimeError`` if no ``named_credential_config`` section is set.
    """
    from datacustomcode.named_credential_config import named_credential_config

    cfg = named_credential_config.named_credential_config
    if cfg is None:
        raise RuntimeError(
            "named_credential_config is not configured. Add a "
            "'named_credential_config' section to config.yaml."
        )
    return cfg.to_object()


class DefaultSparkNamedCredential(SparkNamedCredential):
    """
    Callout for Spark, delegating to the shared implementation.
    """

    CONFIG_NAME = "DefaultSparkNamedCredential"

    def __init__(
        self,
        named_credential: Optional["NamedCredential"] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if named_credential is None:
            named_credential = _build_underlying_named_credential()
        self._named_credential: "NamedCredential" = named_credential

    def request(
        self,
        request: "HTTPRequest",
        body: Optional[str] = None,
    ) -> "HTTPResponse":
        return self._named_credential.request(request, body)

    def request_col(
        self,
        request: "HTTPRequest",
        body: Optional["Column"] = None,
    ) -> "Column":
        """Per-row callout via a client-side Spark UDF.

        Returns a struct ``{status, response, error_code, error_message}`` where
        ``response`` is itself a struct
        ``{status_code, body, headers}`` carrying the callout's HTTP response, so
        a script that selects ``request_col(...)["response"]["status_code"]``
        behaves the same during development and in the Data Cloud runtime. Per-row
        failures populate the error fields instead of aborting the Spark job.
        """
        from pyspark.sql.functions import lit, udf
        from pyspark.sql.types import (
            IntegerType,
            MapType,
            StringType,
            StructField,
            StructType,
        )

        http_response_schema = StructType(
            [
                StructField("status_code", IntegerType(), True),
                StructField("body", StringType(), True),
                StructField("headers", MapType(StringType(), StringType()), True),
            ]
        )
        result_schema = StructType(
            [
                StructField("status", StringType(), True),
                StructField("response", http_response_schema, True),
                StructField("error_code", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        def _callout(body_str: Optional[str]) -> Dict[str, Any]:
            return _invoke_callout_as_struct(self._named_credential, request, body_str)

        body_col = body if body is not None else lit(None).cast(StringType())
        return udf(_callout, result_schema)(body_col)


def _invoke_callout_as_struct(
    named_credential: "NamedCredential",
    request: "HTTPRequest",
    body_str: Optional[str],
) -> Dict[str, Any]:
    """Run one callout and shape it into the shared result struct.

    ``response`` is a nested struct ``{status_code, body, headers}`` where
    ``body`` is the external response verbatim. Transport errors become per-row
    ERROR structs rather than aborting the job.
    """
    try:
        response = named_credential.request(request, body_str)
    except Exception as exc:  # surface any transport error per row
        return {
            "status": _STATUS_ERROR,
            "response": None,
            "error_code": None,
            "error_message": str(exc),
        }

    return {
        "status": _STATUS_SUCCESS,
        "response": {
            "status_code": response.status_code,
            "body": response.body,
            "headers": response.headers,
        },
        "error_code": None,
        "error_message": None,
    }
