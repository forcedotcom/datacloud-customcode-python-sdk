import logging
from typing import List
from uuid import uuid4

logger = logging.getLogger(__name__)


def chunk_text(text: str, chunk_size: int = 1000) -> List[str]:
    """
    Split text into chunks of approximately chunk_size characters.
    Tries to split at sentence boundaries when possible.
    """
    if not text:
        return []

    chunks = []
    current_chunk = ""

    # Split text into sentences (simple split by period)
    sentences = text.split(". ")

    for sentence in sentences:
        if len(current_chunk) + len(sentence) <= chunk_size:
            current_chunk += sentence + ". "
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


def dc_function(request: dict) -> dict:
    logger.info("Inside DC Function")
    logger.info(request)

    items = request["input"]
    output_chunks = []
    current_seq_no = 1  # Start sequence number from 1

    for item in items:
        # Item is DocElement as dict
        logger.info(f"Processing item: {item}")

        text = item.get("text", "")
        metadata = item.get("metadata", {})

        # Create chunks from the text
        text_chunks = chunk_text(text, chunk_size=100)  # Using a larger chunk size

        # Create chunk dictionaries for each text chunk
        for chunk_content in text_chunks:
            chunk_dict = {
                "text": chunk_content,
                "metadata": metadata,
                "seq_no": current_seq_no,
                "chunk_type": "text",
                "chunk_id": str(uuid4()),
                "tag_metadata": {},
                "citations": {},
                "source_record": item,
            }
            output_chunks.append(chunk_dict)
            current_seq_no += 1  # Increment sequence number for next chunk

    logger.info("Completed chunking")
    response = {
        "output": output_chunks,
        "status": {"status_type": "success", "status_message": "Chunking completed"},
    }
    logger.info(response)
    return response


# Test the function
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Create test data with two DocElements
    test_request = {
        "input": [
            {
                "text": (
                    """This is the first sentence of the first document, which is
                    intentionally made longer to test chunking. """
                    """Here is the second sentence of the first document, which is also
                     quite long and should ensure that the chunking function splits
                     this text into two chunks when the chunk size is set to 100."""
                ),
                "metadata": {"source": "test1", "type": "document"},
            },
            {
                "text": (
                    """This is the first sentence of the second document, and it is
                    also extended to be longer than usual for testing purposes. """
                    """The second sentence of the second document is similarly lengthy,
                     so that the chunking function will again create two chunks for
                     this document."""
                ),
                "metadata": {"source": "test2", "type": "document"},
            },
        ]
    }

    # Run the function
    result = dc_function(test_request)

    # Print the results in a more readable format
    print("\nChunking Results:")
    print("----------------")
    for chunk in result["output"]:
        print(f"\nChunk #{chunk['seq_no']}:")
        print(f"Text: {chunk['text'][:100]}...")  # Print first 100 chars of each chunk
        print(f"Source: {chunk['metadata']['source']}")
        print(f"Chunk ID: {chunk['chunk_id']}")
