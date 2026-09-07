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

from enum import Enum
from typing import Dict

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from datacustomcode.named_credential.types.http_method import HTTPMethod


class HTTPRequest(BaseModel):
    """External callout request. The endpoint and its auth are resolved
    server-side from the Named Credential referenced by ``url``, which uses
    ``callout:<NamedCredential>/<path>`` syntax."""

    model_config = ConfigDict(populate_by_name=True)

    url: str = Field(
        ...,
        min_length=1,
        description="Symbolic Named Credential reference, "
        "e.g. 'callout:<NamedCredential>/<path>'",
    )
    method: str = Field(
        default="GET",
        description="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
    )
    headers: Dict[str, str] = Field(default_factory=dict, description="Request headers")

    @field_validator("method", mode="before")
    @classmethod
    def _normalize_method(cls, value: object) -> str:
        # Accept str, this module's HTTPMethod, or http.HTTPMethod (3.11+).
        if isinstance(value, Enum):
            method = str(value.value)
        else:
            method = str(value)
        method = method.upper()
        if method not in {m.value for m in HTTPMethod}:
            supported = ", ".join(m.value for m in HTTPMethod)
            raise ValueError(
                f"Unsupported HTTP method '{method}'. "
                f"Named Credential callouts support {supported}."
            )
        return method
