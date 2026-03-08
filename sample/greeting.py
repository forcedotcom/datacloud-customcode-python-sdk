from typing import Dict, List


def entry_func(fn):
    return fn


@entry_func
def greet(name: str, greeting: str = "Hello") -> str:
    """Generate a personalized greeting message."""
    return f"{greeting}, {name}!"
