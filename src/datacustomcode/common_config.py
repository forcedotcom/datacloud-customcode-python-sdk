# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from abc import ABC, abstractmethod
import os
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
import yaml

DEFAULT_CONFIG_NAME = "config.yaml"


def default_config_file() -> str:
    return os.path.join(os.path.dirname(__file__), DEFAULT_CONFIG_NAME)


class ForceableConfig(BaseModel):
    force: bool = Field(
        default=False,
        description="If True, this takes precedence over parameters passed to the "
        "initializer of the client",
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


class BaseConfig(ABC, BaseModel):
    @abstractmethod
    def update(self, other: Any) -> "BaseConfig": ...

    def load(self, config_path: str) -> "BaseConfig":
        """Load configuration from a YAML file and merge with existing config"""
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        loaded_config = self.__class__.model_validate(config_data)
        self.update(loaded_config)
        return self
