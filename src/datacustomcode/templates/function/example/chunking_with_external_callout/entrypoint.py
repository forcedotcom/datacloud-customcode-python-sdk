#!/usr/bin/env python3
# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2

"""
Document Chunking with a Gemini Named Credential Callout

Splits each input document into paragraph-sized chunks and classifies every
chunk via Google's Gemini ``generateContent`` API, reached through a Named
Credential (``callout:gemini``) so the endpoint URL and API key are resolved
outside this code. The classification is attached to each chunk as citations.
"""

import json
import logging

from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.chunking import (
    ChunkType,
    SearchIndexChunkingV1Output,
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
)
from datacustomcode.named_credential.types.http_method import HTTPMethod
from datacustomcode.named_credential.types.http_request_builder import (
    HTTPRequestBuilder,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CALLOUT_URL = "callout:gemini"

_ANALYSIS_FIELDS = ("summary", "category", "sentiment")

_PROMPT = (
    "Analyze the following document chunk and classify it. Respond with its "
    "one-sentence summary, a single-word category, overall sentiment "
    "(positive, negative, or neutral), and up to five key topics.\n\nChunk:\n"
)

# Force Gemini to return the classification as JSON in a fixed shape.
_GENERATION_CONFIG = {
    "responseMimeType": "application/json",
    "responseSchema": {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "category": {"type": "string"},
            "sentiment": {"type": "string"},
            "topics": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["summary", "category", "sentiment", "topics"],
    },
}


def _chunk_text(text: str, max_words: int = 80) -> list[str]:
    """Split text into paragraph-aligned chunks of at most ``max_words`` words."""
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

    chunks: list[str] = []
    current: list[str] = []
    current_words = 0

    for paragraph in paragraphs:
        paragraph_words = len(paragraph.split())
        if current and current_words + paragraph_words > max_words:
            chunks.append("\n\n".join(current))
            current = []
            current_words = 0
        current.append(paragraph)
        current_words += paragraph_words

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def _extract_model_json(body: str) -> dict:
    """Decode the model's JSON classification from a Gemini response.

    The generated text sits at ``candidates[0].content.parts[0].text`` and is
    itself a JSON string, so decode twice. Any malformed layer yields ``{}``.
    """
    try:
        envelope = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return {}

    try:
        text = envelope["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, TypeError):
        return {}

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _analyze_chunk(chunk_text: str, runtime: Runtime) -> dict[str, str]:
    """Classify one chunk via the Gemini callout and return it as citations."""
    request = (
        HTTPRequestBuilder()
        .set_url(CALLOUT_URL)
        .set_method(HTTPMethod.POST)
        .set_headers({"Content-Type": "application/json", "Accept": "application/json"})
        .set_response_timeout_seconds(60)
        .build()
    )

    payload = {
        "contents": [{"parts": [{"text": _PROMPT + chunk_text}]}],
        "generationConfig": _GENERATION_CONFIG,
    }
    response = runtime.named_credential.request(request, json.dumps(payload))

    # Don't raise: a single failed callout shouldn't abort the whole job.
    if not response.is_success:
        logger.error(f"Gemini callout failed with status {response.status_code}")
        return {"analysis_status": "failed", "http_status": str(response.status_code)}

    data = _extract_model_json(response.body)
    citations = {"analysis_status": "success"}
    for field in _ANALYSIS_FIELDS:
        value = data.get(field)
        citations[field] = str(value) if value is not None else "unavailable"

    topics = data.get("topics")
    if isinstance(topics, list):
        citations["topics"] = ", ".join(str(topic) for topic in topics)

    return citations


def function(
    request: SearchIndexChunkingV1Request, runtime: Runtime
) -> SearchIndexChunkingV1Response:
    """Chunk each input document and classify every chunk via the Gemini API."""
    logger.info(f"Received {len(request.input)} documents to chunk")

    chunks = []
    chunk_id = 1

    for doc in request.input:
        for chunk_text in _chunk_text(doc.text):
            citations = _analyze_chunk(chunk_text, runtime)

            chunk = SearchIndexChunkingV1Output(
                text=chunk_text,
                seq_no=chunk_id,
                chunk_type=ChunkType.TEXT,
                citations=citations,
            )
            chunks.append(chunk)
            chunk_id += 1

    logger.info(f"Produced {len(chunks)} classified chunks")
    return SearchIndexChunkingV1Response(output=chunks)
