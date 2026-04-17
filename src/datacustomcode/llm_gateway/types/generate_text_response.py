from typing import Optional, Dict, Any

from pydantic import BaseModel, Field

class GenerateTextResponse(BaseModel):
    """Response from LLM text generation"""

    version: str = Field(default="v1", description="API version")
    status_code: int = Field(..., description="HTTP status code", ge=0)
    data: Optional[Dict[str, Any]] = Field(default=None, description="Response data")

    @property
    def is_success(self) -> bool:
        """Check if request succeeded."""
        return self.status_code == 200

    @property
    def is_error(self) -> bool:
        """Check if request failed."""
        return not self.is_success

    @property
    def text(self) -> str:
        """Generated text (convenience property)."""
        if self.is_success:
            return self.data.get('generation', {}).get('generatedText', '')
        return ''

    @property
    def error_code(self) -> str:
        """Generated text (convenience property)."""
        if self.is_error:
            return self.data.get('errorCode', self.status_code)
        return ''
