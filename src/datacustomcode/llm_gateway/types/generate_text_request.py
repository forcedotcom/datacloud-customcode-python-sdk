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
    Literal,
    Optional,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
from pydantic.alias_generators import to_camel


class GenerateTextRequest(BaseModel):

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,  # Allows both snake_case and camelCase input
    )

    version: Literal["v1"] = Field(
        default="v1", description="API version, must be 'v1'"
    )
    model_name: str = Field(..., min_length=1, description="Name of the model to use")
    prompt: str = Field(..., description="Input prompt")
    localization: Optional[Dict[str, Any]] = Field(
        default=None, description="Localization settings"
    )
    tags: Optional[Dict[str, Any]] = Field(default=None, description="Additional tags")
