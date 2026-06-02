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


_DEFAULT_LLM_MODEL_ID = "sfdc_ai__DefaultGPT4Omni"
_DEFAULT_LLM_MAX_TOKENS = 200


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
        max_tokens: Optional[int] = None,
    ) -> str:
        return _invoke_llm_gateway(self._llm_gateway, prompt, model_id, max_tokens)

    def llm_gateway_generate_text_col(
        self,
        template: str,
        values: Union[Dict[str, "Column"], "Column"],
        model_id: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> "Column":

        from pyspark.sql.functions import struct, udf
        from pyspark.sql.types import StringType

        if isinstance(values, dict):
            values_col = struct(*[v.alias(k) for k, v in values.items()])
        else:
            values_col = values

        gateway = self._llm_gateway

        def _generate(values_row: Any) -> str:
            if values_row is None:
                return ""
            subs = (
                values_row.asDict()
                if hasattr(values_row, "asDict")
                else dict(values_row)
            )
            prompt = template.format(**subs)
            return _invoke_llm_gateway(gateway, prompt, model_id, max_tokens)

        return udf(_generate, StringType())(values_col)


def _build_underlying_gateway() -> "LLMGateway":
    from datacustomcode.llm_gateway_config import llm_gateway_config

    cfg = llm_gateway_config.llm_gateway_config
    if cfg is None:
        raise RuntimeError(
            "llm_gateway_config is not configured. Add an 'llm_gateway_config' "
            "section to config.yaml."
        )
    return cfg.to_object()


def _invoke_llm_gateway(
    gateway: "LLMGateway",
    prompt: str,
    model_id: Optional[str],
    max_tokens: Optional[int],
) -> str:
    from datacustomcode.llm_gateway.types.generate_text_request_builder import (
        GenerateTextRequestBuilder,
    )

    builder = (
        GenerateTextRequestBuilder()
        .set_prompt(prompt)
        .set_model(model_id or _DEFAULT_LLM_MODEL_ID)
        .set_max_tokens(max_tokens or _DEFAULT_LLM_MAX_TOKENS)
    )
    return gateway.generate_text(builder.build()).text
