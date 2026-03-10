from typing import Dict, List, Optional, TypedDict

from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema


class AnalyzeRequest(TypedDict):
    dataset: str
    filters: Optional[Dict[str, List[str]]]
    limit: Optional[int]


class AnalyzeResponse(TypedDict):
    rows: List[Dict[str, int]]
    total: int


@entry_func
@requestSchema(AnalyzeRequest)
@responseSchema(AnalyzeResponse)
def analyze(request: dict) -> dict:
    """Analyze data with filters and return grouped results."""
    return {}
