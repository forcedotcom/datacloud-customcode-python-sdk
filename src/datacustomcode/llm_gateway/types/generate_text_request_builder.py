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

from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest


class GenerateTextRequestBuilder:
    def __init__(self):
        self._prompt = ""
        self._model_name = ""
        self._localization = None
        self._tags = None

    def set_prompt(self, prompt: str):
        self._prompt = prompt
        return self

    def set_model(self, model_name: str):
        self._model_name = model_name
        return self

    def set_localization(self, localization: dict = None, locale: str = None):
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
            self._localization = {"defaultLocale": locale}
        else:
            raise ValueError("Must provide either localization or locale")

        return self

    def set_tags(self, tags: dict):
        self._tags = tags
        return self

    def build(self) -> GenerateTextRequest:

        request = GenerateTextRequest(
            prompt=self._prompt,
            model_name=self._model_name,
            localization=self._localization,
            tags=self._tags
        )

        return request
