from typing import Optional, TypedDict

from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema


class GreetRequest(TypedDict):
    name: str
    greeting: Optional[str]


class GreetResponse(TypedDict):
    message: str


@entry_func
@requestSchema(GreetRequest)
@responseSchema(GreetResponse)
def greet(request: dict) -> dict:
    """Generate a personalized greeting message."""
    name = request.get("name", "World")
    greeting = request.get("greeting", "Hello")
    return {"message": f"{greeting}, {name}!"}
