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

from pydantic import BaseModel, Field


class GenerateTextResponse(BaseModel):
    """Response from LLM text generation"""

    version: str = Field(default="v1", description="API version")
    status_code: int = Field(..., description="HTTP status code", ge=0)
    data: Optional[Dict[str, Any]] = Field(default=None, description="Response data")

    @property
    def is_success(self) -> bool:
        """Check if request succeeded."""
        return self.status_code == 200

    @property
    def is_error(self) -> bool:
        """Check if request failed."""
        return not self.is_success

    @property
    def text(self) -> str:
        """Generated text (convenience property)."""
        if self.is_success and self.data:
            generation = self.data.get("generation", {})
            if isinstance(generation, dict):
                text = generation.get("generatedText", "")
                return str(text) if text else ""
        return ""

    @property
    def error_code(self) -> str:
        """Generated text (convenience property)."""
        if self.is_error and self.data:
            error_code = self.data.get("errorCode", str(self.status_code))
            return str(error_code)
        return ""
