"""Utility functions for text chunking operations."""

import logging
from typing import (
    Dict,
    List,
    Optional,
)

from datacustomcode.function.feature_types.chunking import SearchIndexChunkingV1Metadata

logger = logging.getLogger(__name__)


def split_text_into_chunks(text: str, max_size: int, overlap: int = 20) -> List[str]:
    """Split text into chunks with overlap, trying to break at natural boundaries.

    Tries to break at natural boundaries in order of preference:
    1. Paragraph boundaries (\\n\\n)
    2. Line boundaries (\\n)
    3. Sentence boundaries (. ! ?)
    4. Word boundaries (space)
    5. Hard cut if no good boundary found

    Args:
        text: Text to split
        max_size: Maximum characters per chunk
        overlap: Number of characters to overlap between chunks

    Returns:
        List of text chunks
    """
    if len(text) <= max_size:
        return [text]

    chunks = []
    start = 0

    while start < len(text):
        # Determine end position for this chunk
        end = start + max_size

        if end >= len(text):
            # Last chunk
            chunks.append(text[start:])
            break

        # Try to find a good breaking point (in order of preference)
        chunk_text = text[start:end]
        break_point = None

        # Try to break at paragraph boundary (\n\n)
        last_paragraph = chunk_text.rfind("\n\n")
        if last_paragraph > max_size * 0.5:  # Only if it's past halfway
            break_point = start + last_paragraph + 2  # +2 to skip the \n\n

        # Try to break at line boundary (\n)
        if break_point is None:
            last_newline = chunk_text.rfind("\n")
            if last_newline > max_size * 0.5:
                break_point = start + last_newline + 1

        # Try to break at sentence boundary (. ! ?)
        if break_point is None:
            for punct in [". ", "! ", "? "]:
                last_sentence = chunk_text.rfind(punct)
                if last_sentence > max_size * 0.5:
                    break_point = start + last_sentence + len(punct)
                    break

        # Try to break at word boundary (space)
        if break_point is None:
            last_space = chunk_text.rfind(" ")
            if last_space > max_size * 0.5:
                break_point = start + last_space + 1

        # If no good breaking point, just hard cut
        if break_point is None:
            break_point = end

        chunks.append(text[start:break_point].strip())

        # Move start position with overlap
        start = max(break_point - overlap, start + 1)

    return chunks


def extract_citations(
    metadata: Optional[SearchIndexChunkingV1Metadata],
) -> Dict[str, str]:
    """Extract citations from document metadata.

    Args:
        metadata: Document metadata containing source DMO fields

    Returns:
        Dictionary of citation key-value pairs
    """
    citations = {}
    if metadata and metadata.source_dmo_fields:
        for key, value in metadata.source_dmo_fields.items():
            citations[key] = str(value)
    return citations