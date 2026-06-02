#!/usr/bin/env python3
"""
Sample Search Index Chunking Customer Function

This function demonstrates the new signature-based invocation with Pydantic models:
- Uses SearchIndexChunkingV1Request/Response (Pydantic models)
- Requires Runtime parameter (for agentic capabilities)
- Type-safe with direct field access (no wrappers)
- Automatic validation and conversion
"""

"""
    You can use your AI models configured in Salesforce to generate texts.

    For testing locally before deploying your code to Data Cloud (datacustomcode run),
    first configure an external client app before using LLM functionality, then configure
    the SDK with your client app credentials.
    
    https://developer.salesforce.com/docs/ai/agentforce/guide/agent-api-get-started.html#create-a-salesforce-app
"""

import logging

from datacustomcode.function.feature_types.chunking import (
    ChunkType,
    SearchIndexChunkingV1Output,
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
)
from datacustomcode.function.runtime import Runtime
from datacustomcode.llm_gateway.types.generate_text_request_builder import (
    GenerateTextRequestBuilder,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _load_prompt_template(runtime: Runtime) -> str:
    """Load the chunking prompt template from file."""
    prompt_file = runtime.file.find_file_path("chunking_prompt.txt")
    with open(prompt_file, "r") as f:
        _prompt_template_cache = f.read()
    logger.info(f"Loaded prompt template from {prompt_file}")
    return _prompt_template_cache


def function(
    request: SearchIndexChunkingV1Request, runtime: Runtime
) -> SearchIndexChunkingV1Response:
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

        # Use LLM to intelligently chunk the document
        # This creates semantic chunks that preserve context and meaning
        prompt = prompt_template.format(text=text)

        builder = GenerateTextRequestBuilder()
        llm_request = (
            builder.set_model("sfdc_ai__DefaultGPT4Turbo").set_prompt(prompt).build()
        )
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
            error_msg = (
                f"LLM chunking failed for document {doc_idx + 1}: {response.error_code}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    # Return Pydantic response
    return SearchIndexChunkingV1Response(output=chunks)
