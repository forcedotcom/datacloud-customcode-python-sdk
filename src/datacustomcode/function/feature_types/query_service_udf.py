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

"""
Pydantic models for Query Service UDF V1

PROVISIONAL: authored while the Query Service integration's dispatch
mechanism (BYOC-DESIGN.md's AlwaysOn/Arrow vs. code-interpreter branch
fork) is still unresolved. This contract shape (one row of scalar
arguments in, one scalar value out) matches a synchronous per-call SQL
UDF invocation and may need revision once the branch decision lands.
"""

from typing import (
    Any,
    List,
    Optional,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class QueryServiceUdfV1Argument(BaseModel):
    """A single positional argument passed into the UDF for one row"""

    name: Optional[str] = Field(
        default=None,
        description="Parameter name as declared in the UDF signature",
        examples=["amount"],
    )
    value: Optional[Any] = Field(
        default=None,
        description="Argument value for this row. Type follows the UDF's declared parameter type.",
        examples=[42.5],
    )
    model_config = ConfigDict(extra="ignore")


class QueryServiceUdfV1Row(BaseModel):
    """One row of arguments to evaluate the UDF against"""

    arguments: List[QueryServiceUdfV1Argument] = Field(
        default_factory=list,
        description="Ordered arguments for this row, matching the UDF signature",
    )
    model_config = ConfigDict(extra="ignore")


class QueryServiceUdfV1Result(BaseModel):
    """Evaluated result for one input row"""

    value: Optional[Any] = Field(
        default=None,
        description="Return value for this row. Type follows the UDF's declared return type.",
        examples=[127.5],
    )
    model_config = ConfigDict(extra="ignore")


class QueryServiceUdfV1Request(BaseModel):
    """Request for Query Service UDF invocation"""

    rows: List[QueryServiceUdfV1Row] = Field(
        default_factory=list, description="Rows to evaluate the UDF against"
    )
    model_config = ConfigDict(extra="ignore")


class QueryServiceUdfV1Response(BaseModel):
    """Response for Query Service UDF invocation"""

    results: List[QueryServiceUdfV1Result] = Field(
        default_factory=list, description="Evaluated results, one per input row"
    )
