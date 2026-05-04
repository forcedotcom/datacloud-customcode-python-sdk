import logging
import uuid

from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.chunking import (
    ChunkType,
    SearchIndexChunkingV1Output,
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Default max chunk size (can be overridden if contract adds max_characters field)
DEFAULT_MAX_CHUNK_SIZE = 50


def split_text_into_chunks(text: str, max_size: int, overlap: int = 20):
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


def function(
    request: SearchIndexChunkingV1Request, runtime: Runtime
) -> SearchIndexChunkingV1Response:
    """Chunk documents into smaller pieces for search indexing.

    Args:
        request: SearchIndexChunkingV1Request with input documents
        runtime: Runtime context (unused but required by contract)

    Returns:
        SearchIndexChunkingV1Response with chunked output
    """
    print(f"Received {len(request.input)} documents to chunk")

    chunks = []
    seq_no = 1

    # Use default max chunk size
    max_chunk_size = DEFAULT_MAX_CHUNK_SIZE

    # Process each document
    for doc_idx, doc in enumerate(request.input):
        text = doc.text
        metadata = doc.metadata

        print(f"Processing document {doc_idx + 1}: {len(text)} characters")

        # Split the text using our simple chunking algorithm
        text_chunks = split_text_into_chunks(text, max_chunk_size, overlap=20)

        # Create chunk outputs
        for chunk_text in text_chunks:
            # Create citations from source_dmo_fields if available
            citations = {}
            if metadata.source_dmo_fields:
                for key, value in metadata.source_dmo_fields.items():
                    citations[key] = str(value)

            chunk_output = SearchIndexChunkingV1Output(
                chunk_id=str(uuid.uuid4()),
                chunk_type=ChunkType.TEXT,
                text=chunk_text.strip(),
                seq_no=seq_no,
                citations=citations,
            )
            chunks.append(chunk_output)

            print(f"Chunk {seq_no}: {len(chunk_text)} chars")
            seq_no += 1

    print(f"Generated {len(chunks)} chunks total")

    return SearchIndexChunkingV1Response(output=chunks)
