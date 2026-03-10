from typing import Dict

from datacustomcode.entry_func import entry_func


@entry_func
def greet(request: Dict[str, str]) -> Dict[str, str]:
    """Generate a personalized greeting message."""
    name = request.get("name", "World")
    greeting = request.get("greeting", "Hello")
    return {"message": f"{greeting}, {name}!"}
