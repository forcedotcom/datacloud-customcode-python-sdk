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

from pydantic import Field

from datacustomcode.common_config import (
    BaseConfig,
    BaseObjectConfig,
    ForceableConfig,
    default_config_file,
)

# This lets all readers and writers to be findable via config
from datacustomcode.io import *  # noqa: F403
from datacustomcode.io.base import BaseDataAccessLayer
from datacustomcode.spark.base import BaseSparkSessionProvider

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from datacustomcode.io.reader.base import BaseDataCloudReader
    from datacustomcode.io.writer.base import BaseDataCloudWriter


_T = TypeVar("_T", bound="BaseDataAccessLayer")


class AccessLayerObjectConfig(BaseObjectConfig, Generic[_T]):
    type_base: ClassVar[Type[BaseDataAccessLayer]] = BaseDataAccessLayer

    def to_object(self, spark: SparkSession) -> _T:
        type_ = self.type_base.subclass_from_config_name(self.type_config_name)
        return cast("_T", type_(spark=spark, **self.options))


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


_P = TypeVar("_P", bound=BaseSparkSessionProvider)


class SparkProviderConfig(BaseObjectConfig, Generic[_P]):
    type_base: ClassVar[Type[BaseSparkSessionProvider]] = BaseSparkSessionProvider

    def to_object(self) -> _P:
        type_ = self.type_base.subclass_from_config_name(self.type_config_name)
        return cast("_P", type_(**self.options))


class ClientConfig(BaseConfig):
    reader_config: Union[AccessLayerObjectConfig["BaseDataCloudReader"], None] = None
    writer_config: Union[AccessLayerObjectConfig["BaseDataCloudWriter"], None] = None
    spark_config: Union[SparkConfig, None] = None
    spark_provider_config: Union[
        SparkProviderConfig[BaseSparkSessionProvider], None
    ] = None

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
        self.spark_provider_config = merge(
            self.spark_provider_config, other.spark_provider_config
        )
        return self


"""Global config object.

This is the object that makes config accessible globally and globally mutable.
"""
config = ClientConfig()
config.load(default_config_file())
