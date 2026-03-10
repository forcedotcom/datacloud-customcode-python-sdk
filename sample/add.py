from typing import Dict

from datacustomcode.entry_func import entry_func


@entry_func
def add(request: Dict[str, int]) -> Dict[str, int]:
    """Add two integers."""
    a = request.get("a", 0)
    b = request.get("b", 0)
    return {"result": a + b}
