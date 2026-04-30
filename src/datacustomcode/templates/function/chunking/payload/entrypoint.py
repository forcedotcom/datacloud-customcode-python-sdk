import logging

from langchain_text_splitters import RecursiveCharacterTextSplitter

from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.chunking import (
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
    SearchIndexChunkOutput,
    SearchIndexStatusResponse,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def function(
    request: SearchIndexChunkingV1Request, runtime: Runtime
) -> SearchIndexChunkingV1Response:
    print(f"Received {len(request.input)} documents to chunk")
    print(f"Max characters per chunk: {request.max_characters}")

    # Initialize RecursiveCharacterTextSplitter
    # It tries to split on: "\n\n", "\n", " ", "" (in that order)
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=request.max_characters,
        chunk_overlap=20,  # Small overlap to maintain context
        length_function=len,
        separators=["\n\n", "\n", " ", ""],
    )

    chunks = []
    chunk_id = 1

    # Process each document
    for doc_idx, doc in enumerate(request.input):
        text = doc.text
        metadata = doc.metadata if hasattr(doc.metadata, "__iter__") else {}

        print(f"📄 Processing document {doc_idx + 1}: {len(text)} characters")

        # Split the text using RecursiveCharacterTextSplitter
        text_chunks = text_splitter.split_text(text)

        # Create chunk outputs
        for chunk_text in text_chunks:
            chunk_output = SearchIndexChunkOutput(
                chunk_id=f"chunk_{chunk_id:04d}",
                chunk_type="text",
                text=chunk_text.strip(),
                seq_no=chunk_id,
                metadata={
                    k: str(v) for k, v in (dict(metadata) if metadata else {}).items()
                },
                tag_metadata={},
                citations={},
            )
            chunks.append(chunk_output)

            print(f"  ✂️  Chunk {chunk_id}: {len(chunk_text)} chars")
            chunk_id += 1

    print(f"✅ Generated {len(chunks)} chunks total")

    return SearchIndexChunkingV1Response(
        output=chunks,
        status=SearchIndexStatusResponse(
            status_type="success",
            status_message=(
                f"Successfully chunked {len(request.input)} documents "
                f"into {len(chunks)} chunks"
            ),
        ),
    )
