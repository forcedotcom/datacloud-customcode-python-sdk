from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


class GenerateTextRequest(BaseModel):
    """Request for LLM text generation"""

    version: Literal["v1"] = Field(default="v1", description="API version, must be 'v1'")
    model_name: str = Field(..., min_length=1, description="Name of the model to use")
    prompt: str = Field(..., min_length=1, max_length=1000, description="Input prompt")
    localization: Optional[Dict[str, Any]] = Field(default=None, description="Localization settings")
    tags: Optional[Dict[str, Any]] = Field(default=None, description="Additional tags")
