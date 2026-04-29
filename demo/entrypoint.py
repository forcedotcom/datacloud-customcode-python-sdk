import logging
from typing import List, Optional, TypedDict

from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema

logger = logging.getLogger(__name__)


class InputItem(TypedDict):
    text: str


class StatusResponse(TypedDict):
    status_type: str
    status_message: str


class ConcatRequest(TypedDict):
    input: List[InputItem]
    separator: Optional[str]


class ConcatResponse(TypedDict):
    output: str
    status: StatusResponse


@entry_func
@requestSchema(ConcatRequest)
@responseSchema(ConcatResponse)
def dc_function(request: dict) -> dict:
    """Concatenate a list of strings from request input into one string.

    Args:
        request: Dictionary containing:
            - input (List[str]): List of strings to concatenate.
            - separator (str, optional): String inserted between items.

    Returns:
        dict: A response dictionary with the concatenated string in `output`
        and processing status metadata in `status`.
    """
    logger.info("Inside DC Function")
    logger.info(request)

    items = request["input"]
    separator = request.get("separator", "")
    concatenated_text = separator.join(item.get("text", "") for item in items)
    response = {
        "output": concatenated_text,
        "status": {
            "status_type": "success",
            "status_message": "Concatenation completed",
        },
    }
    logger.info("Concatenation completed successfully")
    logger.info(response)
    return response
