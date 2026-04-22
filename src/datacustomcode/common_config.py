import os
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from typing import Any

DEFAULT_CONFIG_NAME = "config.yaml"


def default_config_file() -> str:
    return os.path.join(os.path.dirname(__file__), DEFAULT_CONFIG_NAME)


class ForceableConfig(BaseModel):
    force: bool = Field(
        default=False,
        description="If True, this takes precedence over parameters passed to the initializer of the client",
    )


class BaseObjectConfig(ForceableConfig):
    model_config = ConfigDict(validate_default=True, extra="forbid")
    type_config_name: str = Field(
        description="The config name of the object to create",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options passed to the constructor.",
    )


class BaseConfig(BaseModel):
    def load(self, config_path: str) -> "BaseConfig":
        """Load configuration from a YAML file and merge with existing config"""
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        loaded_config = self.__class__.model_validate(config_data)
        self.update(loaded_config)
        return self
