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
    Dict,
    Optional,
    Union,
)

from datacustomcode.mixin import UserExtendableNamedConfigMixin

if TYPE_CHECKING:
    from pyspark.sql import Column


class SparkLLMGateway(ABC, UserExtendableNamedConfigMixin):
    CONFIG_NAME: str

    def __init__(self, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def llm_gateway_generate_text(
        self,
        prompt: str,
        model_id: Optional[str] = None,
    ) -> str:
        """Issue a one-shot LLM Gateway call and return the generated text."""

    @abstractmethod
    def llm_gateway_generate_text_col(
        self,
        template: str,
        values: Union[Dict[str, "Column"], "Column"],
        model_id: Optional[str] = None,
    ) -> "Column":
        """Build a Spark ``Column`` that invokes the LLM Gateway per row and
        yields a struct ``{status, response, error_code, error_message}``.

        Select an individual field, e.g.
        ``llm_gateway_generate_text_col(...)["response"]``. Returning a struct
        means a single failing row doesn't abort the Spark job.
        Failing row leaves the rest of the DataFrame intact — callers can
        inspect ``status`` / ``error_code`` per row instead of having the
        Spark job abort.
        """
