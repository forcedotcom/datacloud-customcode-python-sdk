# Copyright (c) 2026, Salesforce, Inc.
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
Pydantic models for Knowledge Harmonization Post-Processing V1
"""

from typing import (
    Dict,
    Optional,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class KnowledgeHarmonizationPostProcessingV1Metadata(BaseModel):
    """Metadata for the harmonized content element"""

    source_dmo_fields: Optional[Dict[str, Union[str, int, float]]] = Field(
        default=None,
        description=(
            "Source Data Model Object fields as key-value pairs, "
            "e.g. FilePath__c, ContentType__c, LastModified__c."
        ),
        examples=[
            {
                "FilePath__c": "quarterly_report.pdf",
                "ContentType__c": "application/pdf",
                "LastModified__c": "2026-08-12T00:00:00.000000",
            }
        ],
    )
    model_config = ConfigDict(extra="ignore")


class KnowledgeHarmonizationPostProcessingV1DocElement(BaseModel):
    """The harmonized content element to be transformed"""

    text: str = Field(
        default="",
        description=(
            "Harmonized content produced by the built-in pipeline steps, "
            "before the extension runs."
        ),
        examples=["<h1>Quarterly Report</h1><p>Revenue grew 12%.</p>"],
    )
    metadata: Optional[KnowledgeHarmonizationPostProcessingV1Metadata] = Field(
        default=None,
        description="Source document metadata. Optional, may be absent.",
    )
    model_config = ConfigDict(extra="ignore")


class KnowledgeHarmonizationPostProcessingV1Output(BaseModel):
    """The transformed content element returned by the function"""

    text: str = Field(
        default="",
        description=(
            "Transformed content. Replaces the harmonized content at the same "
            "location. On any failure the caller falls back to the original."
        ),
        examples=["<h1>Quarterly Report</h1><p>Revenue grew 12% (redacted).</p>"],
    )
    model_config = ConfigDict(extra="ignore")


class KnowledgeHarmonizationPostProcessingV1Request(BaseModel):
    """Request for Knowledge Harmonization Post-Processing"""

    input: KnowledgeHarmonizationPostProcessingV1DocElement = Field(
        ...,
        description=(
            "The single harmonized content element to transform. "
            "One file per call, not a list."
        ),
    )
    properties: Optional[Dict[str, Union[str, int, float, bool]]] = Field(
        default=None,
        description=(
            "Optional per-call key-value settings. Advisory: functions ignore "
            "keys they do not know. Do not put secrets or PII here."
        ),
        examples=[{"mode": "redact", "locale": "en-US"}],
    )
    model_config = ConfigDict(extra="ignore")


class KnowledgeHarmonizationPostProcessingV1Response(BaseModel):
    """Response for Knowledge Harmonization Post-Processing"""

    output: KnowledgeHarmonizationPostProcessingV1Output = Field(
        ...,
        description="The single transformed content element (1:1 with Request.input).",
    )
    model_config = ConfigDict(extra="ignore")
