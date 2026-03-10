from typing import Dict, List

from datacustomcode.entry_func import entry_func


@entry_func
def transform(request: Dict[str, List[Dict[str, str]]]) -> Dict[str, List[Dict[str, str]]]:
    """Transform a list of records by remapping keys."""
    return {}
