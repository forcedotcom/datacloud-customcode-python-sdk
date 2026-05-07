#!/usr/bin/env python3
"""
Sample Search Index Chunking Customer Function

This function demonstrates the new signature-based invocation with Pydantic models:
- Uses SearchIndexChunkingV1Request/Response (Pydantic models)
- Requires Runtime parameter (for agentic capabilities)
- Type-safe with direct field access (no wrappers)
- Automatic validation and conversion
"""

from datacustomcode.function.runtime import Runtime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



from datacustomcode.function.feature_types.chunking import (
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
    SearchIndexChunkingV1Output,
    ChunkType
)

from datacustomcode.llm_gateway.types.generate_text_request_builder import GenerateTextRequestBuilder


# Load prompt template once at module load time
PROMPT_TEMPLATE = None

def _load_prompt_template(runtime: Runtime) -> str:
    """Load the chunking prompt template from file."""
    global PROMPT_TEMPLATE
    if PROMPT_TEMPLATE is None:
        prompt_file = runtime.file.find_file_path("chunking_prompt.txt")
        with open(prompt_file, 'r') as f:
            PROMPT_TEMPLATE = f.read()
        logger.info(f"Loaded prompt template from {prompt_file}")
    return PROMPT_TEMPLATE


def function(request: SearchIndexChunkingV1Request, runtime: Runtime) -> SearchIndexChunkingV1Response:
    """
    Chunk documents for Search Index.

    Args:
        request: SearchIndexChunkingV1Request with input documents
        runtime: Runtime instance for agentic capabilities (future use)

    Returns:
        SearchIndexChunkingV1Response with chunked output
    """
    logger.info(f"Received {len(request.input)} documents to chunk")

    # Load prompt template (cached after first call)
    prompt_template = _load_prompt_template(runtime)

    chunks = []
    chunk_id = 1

    # Process each document
    for doc_idx, doc in enumerate(request.input):
        # Direct field access - no wrappers!
        text = doc.text
        metadata = doc.metadata

        # Use LLM to intelligently chunk the document
        # This creates semantic chunks that preserve context and meaning
        prompt = prompt_template.format(text=text)

        builder = GenerateTextRequestBuilder()
        llm_request = builder.set_model("sfdc_ai__DefaultGPT4Turbo").set_prompt(prompt).build()
        response = runtime.llm_gateway.generate_text(llm_request)

        if response.is_success:
            # Parse LLM response to extract chunks
            llm_chunks = response.text.split("---CHUNK---")
            llm_chunks = [chunk.strip() for chunk in llm_chunks if chunk.strip()]


            # Create chunk outputs
            for chunk_text in llm_chunks:
                chunk = SearchIndexChunkingV1Output(
                    text=chunk_text,
                    seq_no=chunk_id,
                    chunk_type=ChunkType.TEXT,
                    citations={},
                )
                chunks.append(chunk)
                chunk_id += 1

        else:
            # LLM chunking failed - log error and raise exception
            error_msg = f"LLM chunking failed for document {doc_idx + 1}: {response.error_code}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    # Return Pydantic response
    return SearchIndexChunkingV1Response(output=chunks)