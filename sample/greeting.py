from datacustomcode.entry_func import entry_func


@entry_func
def greet(name: str, greeting: str = "Hello") -> str:
    """Generate a personalized greeting message."""
    return f"{greeting}, {name}!"
