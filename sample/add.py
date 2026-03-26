from typing import TypedDict

from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema


class AddRequest(TypedDict):
    a: int
    b: int


class AddResponse(TypedDict):
    result: int


@entry_func
@requestSchema(AddRequest)
@responseSchema(AddResponse)
def add(request: dict) -> dict:
    """Add two integers."""
    a = request.get("a", 0)
    b = request.get("b", 0)
    return {"result": a + b}
