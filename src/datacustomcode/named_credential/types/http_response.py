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

from typing import Dict

from pydantic import BaseModel, Field


class HTTPResponse(BaseModel):
    """Response from a Named Credential external callout."""

    status_code: int = Field(..., description="HTTP status code", ge=0)
    headers: Dict[str, str] = Field(
        default_factory=dict, description="Response headers"
    )
    body: str = Field(
        default="",
        description="Raw response body, verbatim (any format). The SDK does not "
        "parse it; the caller decodes as needed (e.g. json.loads). Empty string "
        "if the response had no body.",
    )

    @property
    def is_success(self) -> bool:
        """Check if the request succeeded (2xx)."""
        return 200 <= self.status_code < 300

    @property
    def is_error(self) -> bool:
        """Check if the request failed."""
        return not self.is_success
