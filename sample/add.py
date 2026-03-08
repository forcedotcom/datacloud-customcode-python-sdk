from datacustomcode.entry_func import entry_func


@entry_func
def add(a: int, b: int = 0) -> int:
    """Add two integers."""
    return a + b
