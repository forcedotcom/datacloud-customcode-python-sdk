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
    Union,
)

from datacustomcode.llm_gateway.spark_base import SparkLLMGateway

if TYPE_CHECKING:
    from pyspark.sql import Column

    from datacustomcode.llm_gateway.base import LLMGateway
    from datacustomcode.llm_gateway.types.generate_text_response import (
        GenerateTextResponse,
    )


_DEFAULT_LLM_MODEL_ID = "sfdc_ai__DefaultGPT4Omni"

_STATUS_SUCCESS = "SUCCESS"
_STATUS_ERROR = "ERROR"


class DefaultSparkLLMGateway(SparkLLMGateway):

    CONFIG_NAME = "DefaultSparkLLMGateway"

    def __init__(
        self,
        llm_gateway: Optional["LLMGateway"] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if llm_gateway is None:
            llm_gateway = _build_underlying_gateway()
        self._llm_gateway: "LLMGateway" = llm_gateway

    def llm_gateway_generate_text(
        self,
        prompt: str,
        model_id: Optional[str] = None,
    ) -> str:
        return _invoke_llm_gateway(self._llm_gateway, prompt, model_id)

    def llm_gateway_generate_text_col(
        self,
        template: str,
        values: Union[Dict[str, "Column"], "Column"],
        model_id: Optional[str] = None,
    ) -> "Column":
        """Build a per-row UDF that returns a struct ``{status, response,
        error_code, error_message}`` so per-row failures do not abort the
        Spark job. Callers select the field they want, e.g.
        ``llm_gateway_generate_text_col(...)["response"]``.
        """
        from pyspark.sql.functions import struct, udf
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
        )

        if isinstance(values, dict):
            values_col = struct(*[v.alias(k) for k, v in values.items()])
        else:
            values_col = values

        gateway = self._llm_gateway
        result_schema = StructType(
            [
                StructField("status", StringType(), True),
                StructField("response", StringType(), True),
                StructField("error_code", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        def _generate(values_row: Any) -> Dict[str, Optional[str]]:
            if values_row is None:
                return {
                    "status": _STATUS_ERROR,
                    "response": None,
                    "error_code": None,
                    "error_message": "values column was null for this row",
                }
            subs = (
                values_row.asDict()
                if hasattr(values_row, "asDict")
                else dict(values_row)
            )
            prompt = template.format(**subs)
            return _invoke_llm_gateway_as_struct(gateway, prompt, model_id)

        return udf(_generate, result_schema)(values_col)


def _build_underlying_gateway() -> "LLMGateway":
    from datacustomcode.llm_gateway_config import llm_gateway_config

    cfg = llm_gateway_config.llm_gateway_config
    if cfg is None:
        raise RuntimeError(
            "llm_gateway_config is not configured. Add an 'llm_gateway_config' "
            "section to config.yaml."
        )
    return cfg.to_object()


def _call_llm_gateway(
    gateway: "LLMGateway",
    prompt: str,
    model_id: Optional[str],
) -> "GenerateTextResponse":
    """Build the request and dispatch it to the underlying gateway."""
    from datacustomcode.llm_gateway.types.generate_text_request_builder import (
        GenerateTextRequestBuilder,
    )

    request = (
        GenerateTextRequestBuilder()
        .set_prompt(prompt)
        .set_model(model_id or _DEFAULT_LLM_MODEL_ID)
        .build()
    )
    return gateway.generate_text(request)


def _invoke_llm_gateway(
    gateway: "LLMGateway",
    prompt: str,
    model_id: Optional[str],
) -> str:
    from datacustomcode.llm_gateway.errors import LLMGatewayCallError

    response = _call_llm_gateway(gateway, prompt, model_id)
    if response.is_error:
        raise LLMGatewayCallError(
            f"LLM Gateway call failed: status_code={response.status_code}, "
            f"error_code={response.error_code!r}, "
            f"message={response.data!r}",
            status=response.status_code,
            error_code=response.error_code or None,
            error_message=str(response.data) if response.data else None,
        )
    return response.text


def _invoke_llm_gateway_as_struct(
    gateway: "LLMGateway",
    prompt: str,
    model_id: Optional[str],
) -> Dict[str, Optional[str]]:
    response = _call_llm_gateway(gateway, prompt, model_id)
    if response.is_error:
        return {
            "status": _STATUS_ERROR,
            "response": None,
            "error_code": response.error_code or None,
            "error_message": str(response.data) if response.data else None,
        }
    return {
        "status": _STATUS_SUCCESS,
        "response": response.text,
        "error_code": None,
        "error_message": None,
    }
