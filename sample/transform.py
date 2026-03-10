from typing import Dict, List, TypedDict

from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema


class TransformRequest(TypedDict):
    records: List[Dict[str, str]]
    key_mapping: Dict[str, str]


class TransformResponse(TypedDict):
    transformed: List[Dict[str, str]]
    count: int


@entry_func
@requestSchema(TransformRequest)
@responseSchema(TransformResponse)
def transform(request: dict) -> dict:
    """Transform a list of records by remapping keys."""
    return {}
