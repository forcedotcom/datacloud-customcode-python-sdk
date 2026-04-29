import logging

from datacustomcode.function import Runtime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


from datacustomcode.function.feature_types.chunking import (
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
    SearchIndexChunkOutput,
    SearchIndexStatusResponse
)


def function(request: SearchIndexChunkingV1Request, runtime: Runtime) -> SearchIndexChunkingV1Response:
    print(f"Received {len(request.input)} documents to chunk")
    print(f"Max characters per chunk: {request.max_characters}")

    chunks = []
    chunk_id = 1

    # Process each document
    for doc_idx, doc in enumerate(request.input):
        # Access fields - works identically in both Pydantic and betterproto!
        text = doc.text
        metadata = doc.metadata if hasattr(doc.metadata, '__iter__') else {}

        print(f"📄 Processing document {doc_idx + 1}: {len(text)} characters")

        # Chunk the text
        max_chars = request.max_characters
        chunk_start = 0

        while chunk_start < len(text):
            chunk_end = min(chunk_start + max_chars, len(text))
            chunk_text = text[chunk_start:chunk_end]

            # Try to break at word boundary if not at end
            if chunk_end < len(text) and not text[chunk_end].isspace():
                # Look for last space in chunk
                last_space = chunk_text.rfind(' ')
                if last_space > max_chars * 0.8:  # Only if space is in last 20%
                    chunk_end = chunk_start + last_space
                    chunk_text = text[chunk_start:chunk_end]


            # Create ChunkOutput object
            chunk_output = SearchIndexChunkOutput(
                chunk_id=f"chunk_{chunk_id:04d}",
                chunk_type="text",
                text=chunk_text.strip(),
                seq_no=chunk_id,
                metadata={k: str(v) for k, v in (dict(metadata) if metadata else {}).items()},
                tag_metadata={},
                citations={}
            )
            chunks.append(chunk_output)

            print(f"  ✂️  Chunk {chunk_id}: {len(chunk_text)} chars")
            chunk_id += 1
            chunk_start = chunk_end

    print(f"✅ Generated {len(chunks)} chunks total")

    # Return UdsChunkingV1BatchResponse object
    return SearchIndexChunkingV1Response(
        output=chunks,
        status=SearchIndexStatusResponse(
            status_type="success",
            status_message=f"Successfully chunked {len(request.input)} documents into {len(chunks)} chunks"
        )
    )
