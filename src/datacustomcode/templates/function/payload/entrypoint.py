import logging

from utility import extract_citations, split_text_into_chunks

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
            citations = extract_citations(metadata)

            chunk_output = SearchIndexChunkingV1Output(
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