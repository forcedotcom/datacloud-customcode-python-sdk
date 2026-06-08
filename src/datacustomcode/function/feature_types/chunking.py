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
    Optional,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class ElementType(str, Enum):
    """Element type enumeration"""

    TEXT = "text"
    TITLE = "title"
    TABLE = "table"
    IMAGE = "image"
    LIST_ITEM = "list_item"
    CODE_SNIPPET = "code_snippet"
    PAGE_METADATA = "page_metadata"


class ChunkType(str, Enum):
    TEXT = "text"


class SearchIndexChunkingV1PrependField(BaseModel):
    """Field to prepend to chunk content"""

    dmo_name: Optional[str] = Field(
        default=None, description="Data Model Object name", examples=["udmo_1__dlm"]
    )
    field_name: Optional[str] = Field(
        default=None,
        description="Field name to prepend",
        examples=["ResolvedFilePath__c"],
    )
    value: Optional[str] = Field(
        default=None,
        description="Field value to prepend",
        examples=["udlo_1__dll:quarterly_report.pdf"],
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1TranscriptField(BaseModel):
    """Transcript timing and speaker metadata for audio/video documents"""

    speaker: Optional[str] = Field(
        default=None,
        description="Speaker name for audio/video transcripts",
        examples=["Agent"],
    )
    start_timestamp: Optional[float] = Field(
        default=None,
        description="Start timestamp of the audio/video clip",
        examples=["1.0"],
    )
    end_timestamp: Optional[float] = Field(
        default=None,
        description="End timestamp of the audio/video clip",
        examples=["8.75"],
    )
    model_config = ConfigDict(extra="ignore")


class SearchIndexChunkingV1Metadata(BaseModel):
    """Metadata for input documents."""

    type: Optional[ElementType] = Field(
        default=ElementType.TEXT,
        description=(
            "Element type of the chunk input. Currently only 'text' is supported."
        ),
        examples=["text"],
    )
    page_number: Optional[int] = Field(
        default=None,
        description=("Page number in the source document."),
        examples=[1],
    )
    transcript_fields: Optional[SearchIndexChunkingV1TranscriptField] = Field(
        default=None,
        description=(
            "Speaker and timestamp metadata for audio/video transcripts. "
            "Optional — only present when the source document is a transcript."
        ),
    )
    text_as_html: Optional[str] = Field(
        default=None,
        description=("Table represented as HTML"),
        examples=["<p>Online Remittance Instructions</p>"],
    )
    source_dmo_fields: Optional[Dict[str, Union[str, int, float]]] = Field(
        default=None,
        description=(
            "Source Data Model Object fields as key-value pairs. "
            "Values can be string, int, or float."
        ),
        examples=[
            {
                "FilePath__c": "quarterly_report.pdf",
                "Size__c": 1377454.0,
                "ContentType__c": "pdf",
                "LastModified__c": "2026-03-25T02:01:24.918000",
            }
        ],
    )
    prepend: Optional[List[SearchIndexChunkingV1PrependField]] = Field(
        default=None,
        description=(
            "List of DMO fields whose values are prepended to the chunk "
            "text before indexing"
        ),
    )
    image_base64: Optional[str] = Field(
        default=None,
        description=(
            "Base64-encoded image data associated with this chunk. "
            "Optional — only applicable for image-type document elements."
        ),
    )
    image_mime_type: Optional[str] = Field(
        default=None,
        description=(
            "MIME type of the associated image (e.g., 'image/png', 'image/jpeg'). "
            "Optional — should be provided alongside image_base64 when present."
        ),
        examples=["image/png", "image/jpeg"],
    )
    image_type: Optional[str] = Field(
        default=None,
        description=(
            "Semantic category of the image content"
            "(e.g., 'diagram', 'screenshot', 'chart'). Optional."
        ),
        examples=["diagram", "screenshot"],
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
    metadata: Optional[SearchIndexChunkingV1Metadata] = Field(
        default=None,
        description=(
            "Source document metadata. Optional — may be absent if no "
            "metadata is available for the document element."
        ),
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
        default=1,
        description=(
            "Sequential order of this chunk within the output "
            "Represents chunk ordering within the source document (1-based)."
        ),
        ge=1,
        examples=[1],
    )
    chunk_type: ChunkType = Field(
        default=ChunkType.TEXT,
        description="Type of chunk. Fixed value — always 'text'.",
        examples=["text"],
    )
    citations: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Citation metadata associated with this chunk as key-value "
            "pairs. Optional — defaults to None if no citations are present."
        ),
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
