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

from datacustomcode.llm_gateway.base import LLMGateway
from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest
from datacustomcode.llm_gateway.types.generate_text_response import GenerateTextResponse


class DefaultLLMGateway(LLMGateway):
    def generate_text(
            self,
            request: GenerateTextRequest
    ) -> GenerateTextResponse:


        response_data = {
            'generation' : {'generatedText' : "I am dreaming!!"},
        }

        return GenerateTextResponse(200, {"data": response_data})