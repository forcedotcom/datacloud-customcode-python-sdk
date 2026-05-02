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
Pydantic models for Search Index Chunking V1
"""
from enum import Enum
from typing import (
    Dict,
    List,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class DocumentType(str, Enum):
    """Document type enumeration"""

    TEXT = "Text"
    TITLE = "Title"
    TABLE = "Table"
    IMAGE = "Image"
    LIST_ITEM = "ListItem"
    CODE_SNIPPET = "CodeSnippet"
    PAGE_METADATA = "PageMetadata"


class ChunkType(str, Enum):
    TEXT = "text"


class SearchIndexChunkingV1PrependField(BaseModel):
    """Field to prepend to chunk content"""

    dmo_name: str = Field(
        default="", description="Data Model Object name", examples=["udmo_1__dlm"]
    )
    field_name: str = Field(
        default="",
        description="Field name to prepend",
        examples=["ResolvedFilePath__c"],
    )
    value: str = Field(
        default="",
        description="Field value to prepend",
        examples=["udlo_1__dll:quarterly_report.pdf"],
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1TranscriptField(BaseModel):
    """Field to prepend to chunk content"""

    speaker: str = Field(
        default="",
        description="Speaker name for audio/video transcripts",
        examples=["Agent"],
    )
    start_timestamp: str = Field(
        default="",
        description="Start timestamp in ISO8601 format: YYYY-MM-DDTHH:MM:SS.ffffff",
        examples=["2026-03-25T02:01:24.918000"],
    )
    end_timestamp: str = Field(
        default="",
        description="End timestamp in ISO8601 format: YYYY-MM-DDTHH:MM:SS.ffffff",
        examples=["2026-03-25T02:01:30.500000"],
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1Metadata(BaseModel):
    """Metadata for input documents"""

    type: DocumentType = Field(
        default=DocumentType.TEXT, description="Document type (Text)", examples=["Text"]
    )
    transcript_fields: SearchIndexChunkingV1TranscriptField = Field(
        default_factory=SearchIndexChunkingV1TranscriptField,
        description=(
            "Transcript information. Will only be there in case of audio-video files"
        ),
    )
    page_number: int = Field(
        default=0,
        description="Page number in the source document (0-based)",
        examples=[1],
    )
    text_as_html: str = Field(
        default="",
        description="HTML representation of the document text",
        examples=["<p>Online Remittance Instructions</p>"],
    )
    source_dmo_fields: Dict[str, Union[str, int]] = Field(
        default_factory=dict,
        description=(
            "Source Data Model Object fields as key-value pairs "
            "(values can be string or int)"
        ),
        examples=[
            {
                "FilePath__c": "quarterly_report.pdf",
                "Size__c": 1377454,
                "ContentType__c": "pdf",
                "LastModified__c": "2026-03-25T02:01:24.918000",
            }
        ],
    )
    prepend: List[SearchIndexChunkingV1PrependField] = Field(
        default_factory=list, description="List of fields to prepend to each chunk"
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1DocElement(BaseModel):
    """Document element to be chunked"""

    text: str = Field(
        default="",
        description="Text content to be chunked",
        examples=[
            (
                "Online Remittance Instructions\n\n"
                "Transfer proceeds from the sale of your ESOP/RSUs easily."
            )
        ],
    )
    metadata: SearchIndexChunkingV1Metadata = Field(
        default_factory=SearchIndexChunkingV1Metadata,
        description="Source document metadata",
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1Output(BaseModel):
    """Output chunk from the chunking process"""

    text: str = Field(
        default="",
        description="Chunk text content",
        examples=["Online Remittance Instructions"],
    )
    seq_no: int = Field(
        default=0, description="Sequential chunk number (1-based)", ge=1, examples=[1]
    )
    chunk_id: str = Field(
        default="",
        description="Unique identifier for this chunk (UUID format)",
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    chunk_type: ChunkType = Field(
        default=ChunkType.TEXT,
        description="Type of chunk (e.g., 'text')",
        examples=["text"],
    )
    citations: Dict[str, str] = Field(
        default_factory=dict,
        description="Citation information as key-value pairs",
        examples=[{"source": "quarterly_report.pdf"}],
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1Request(BaseModel):
    """Request for Search Index Chunking"""

    input: List[SearchIndexChunkingV1DocElement] = Field(
        default_factory=list, description="List of documents to be chunked"
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1Response(BaseModel):
    """Batch response for UDS chunking"""

    output: List[SearchIndexChunkingV1Output] = Field(
        default_factory=list, description="Flat list of chunks from all docs"
    )
    model_config = ConfigDict(extra="ignore")
