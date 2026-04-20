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

from typing import Any, Dict

from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse


class GenerateTextResponseBuilder:
    def __init__(self):
        self._version = "v1"  # Hardcoded default for your SDK
        self._status_code = None
        self._data = None

    def set_status_code(self, status_code: int):
        self._status_code = status_code
        return self

    def set_data(self, data: dict):
        self._data = data
        return self

    @staticmethod
    def build(response_dict: Dict[str, Any]) -> GenerateTextResponse:
        return GenerateTextResponse.model_validate(response_dict)
