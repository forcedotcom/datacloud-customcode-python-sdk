from typing import Dict, List

from datacustomcode.entry_func import entry_func


@entry_func
def analyze(request: Dict[str, Dict[str, int]]) -> Dict[str, List[int]]:
    """Analyze data with nested configuration and return grouped results."""
    return {}
