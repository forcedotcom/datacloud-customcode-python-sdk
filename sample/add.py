from typing import Dict, List


def entry_func(fn):
    return fn


@entry_func
def add(a: int, b: int = 0) -> int:
    """Add two integers."""
    return a + b
