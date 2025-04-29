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
from __future__ import annotations

import os
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
import yaml

# This lets all readers and writers to be findable via config
from datacustomcode.io import *  # noqa: F403
from datacustomcode.io.base import BaseDataAccessLayer
from datacustomcode.io.reader.base import BaseDataCloudReader  # noqa: TCH001
from datacustomcode.io.writer.base import BaseDataCloudWriter  # noqa: TCH001

DEFAULT_CONFIG_NAME = "config.yaml"


if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class ForceableConfig(BaseModel):
    force: bool = Field(
        default=False,
        description="If True, this takes precedence over parameters passed to the "
        "initializer of the client.",
    )


_T = TypeVar("_T", bound="BaseDataAccessLayer")


class AccessLayerObjectConfig(ForceableConfig, Generic[_T]):
    model_config = ConfigDict(validate_default=True, extra="forbid")
    type_base: ClassVar[Type[BaseDataAccessLayer]] = BaseDataAccessLayer
    type_config_name: str = Field(
        description="The config name of the object to create. "
        "For metrics, this would might be 'ipmnormal'. For custom classes, you can "
        "assign a name to a class variable `CONFIG_NAME` and reference it here.",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options passed to the constructor.",
    )

    def to_object(self, spark: SparkSession) -> _T:
        type_ = self.type_base.subclass_from_config_name(self.type_config_name)
        return cast(_T, type_(spark=spark, **self.options))


class SparkConfig(ForceableConfig):
    app_name: str = Field(
        description="The name of the Spark application.",
    )
    master: Union[str, None] = Field(
        default=None,
        description="The Spark master URL.",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options passed to the SparkSession constructor.",
    )


class ClientConfig(BaseModel):
    reader_config: Union[AccessLayerObjectConfig[BaseDataCloudReader], None] = None
    writer_config: Union[AccessLayerObjectConfig[BaseDataCloudWriter], None] = None
    spark_config: Union[SparkConfig, None] = None

    def update(self, other: ClientConfig) -> ClientConfig:
        """Merge this ClientConfig with another, respecting force flags.

        Args:
            other: Another ClientConfig to merge with this one

        Returns:
            Self, with updated values from the other config based on force flags.
        """
        TypeVarT = TypeVar("TypeVarT", bound=ForceableConfig)

        def merge(
            config_a: Union[TypeVarT, None], config_b: Union[TypeVarT, None]
        ) -> Union[TypeVarT, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.reader_config = merge(self.reader_config, other.reader_config)
        self.writer_config = merge(self.writer_config, other.writer_config)
        self.spark_config = merge(self.spark_config, other.spark_config)
        return self

    def load(self, config_path: str) -> ClientConfig:
        """Load a config from a file and update this config with it.

        Args:
            config_path: The path to the config file

        Returns:
            Self, with updated values from the loaded config.
        """
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)
        loaded_config = ClientConfig.model_validate(config_data)

        return self.update(loaded_config)


config = ClientConfig()
"""Global config object.

This is the object that makes config accessible globally and globally mutable.
"""


def _defaults() -> str:
    return os.path.join(os.path.dirname(__file__), DEFAULT_CONFIG_NAME)


config.load(_defaults())
