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
Pydantic models for byoc-function-proto (uds_chunking.proto)
Auto-generated - validation rules from buf.validate
"""

from typing import (
    Any,
    Dict,
    List,
    Literal,
)

from pydantic import BaseModel, Field


class SearchIndexDocElement(BaseModel):
    """Document element to be chunked"""

    text: str = Field(..., description="Text content to be chunked")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Source document metadata"
    )


class SearchIndexChunkOutput(BaseModel):
    """Output chunk from the chunking process"""

    chunk_id: str = Field(..., description="UUID for this chunk")
    chunk_type: str = Field(..., description="Type: 'text'")
    text: str = Field(..., description="Chunk text content")
    seq_no: int = Field(..., description="Sequential chunk number (1-based)")
    metadata: Dict[str, str] = Field(
        default_factory=dict, description="Metadata from source (DMO fields)"
    )
    tag_metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional tags"
    )
    citations: Dict[str, Any] = Field(
        default_factory=dict, description="Citation information"
    )


class SearchIndexStatusResponse(BaseModel):
    """Status response for operation"""

    status_type: str = Field(..., description="'success' or 'error'")
    status_message: str = Field(..., description="Human-readable status")


class SearchIndexChunkingV1Request(BaseModel):
    """Batch request for UDS chunking"""

    input: List[SearchIndexDocElement] = Field(
        ..., min_length=1, description="List of documents (min 1)"
    )
    max_characters: int = Field(..., description="Max chars per chunk (default: 100)")
    additional_params: Dict[str, Any] = Field(
        default_factory=dict, description="Future extension point"
    )


class SearchIndexChunkingV1Response(BaseModel):
    """Batch response for UDS chunking"""
    output: List[SearchIndexChunkOutput] = Field(
        default_factory=list, description="Flat list of chunks from all docs"
    )
    status: SearchIndexStatusResponse = Field(..., description="Overall operation status")
