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

from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest


class GenerateTextRequestBuilder:
    def __init__(self) -> None:
        self._prompt = ""
        self._model_name = ""
        self._localization: Optional[Dict[str, Any]] = None
        self._tags: Optional[Dict[str, Any]] = None

    def set_prompt(self, prompt: str) -> "GenerateTextRequestBuilder":
        self._prompt = prompt
        return self

    def set_model(self, model_name: str) -> "GenerateTextRequestBuilder":
        self._model_name = model_name
        return self

    def set_localization(
        self,
        localization: Optional[Dict[str, Any]] = None,
        locale: Optional[str] = None,
    ) -> "GenerateTextRequestBuilder":
        """
        Set localization either from a dict or a simple locale string.

        Args:
          localization: Full localization dict (if provided, locale is ignored)
          locale: Simple locale string for defaultLocale only

        Returns:
            self for method chaining
        """

        if localization is not None:
            self._localization = localization
        elif locale is not None:
            self._localization = {
                "defaultLocale": locale,
                "inputLocales": [{"locale": locale, "probability": 1.0}],
                "expectedLocales": [locale],
            }
        else:
            raise ValueError("Must provide either localization or locale")

        return self

    def set_tags(self, tags: Dict[str, Any]) -> "GenerateTextRequestBuilder":
        self._tags = tags
        return self

    def build(self) -> GenerateTextRequest:

        request = GenerateTextRequest(
            prompt=self._prompt,
            model_name=self._model_name,
            localization=self._localization,
            tags=self._tags,
        )

        return request
