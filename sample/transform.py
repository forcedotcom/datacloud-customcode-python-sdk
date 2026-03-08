from typing import Dict, List


def entry_func(fn):
    return fn


@entry_func
def transform(
    records: List[Dict[str, int]],
    key_mapping: Dict[str, str],
    drop_nulls: bool = True,
) -> List[Dict[str, str]]:
    """Transform a list of records by remapping keys."""
    return []
