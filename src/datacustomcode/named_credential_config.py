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

from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
    Union,
)

from datacustomcode.common_config import (
    BaseConfig,
    BaseObjectConfig,
    default_config_file,
)
from datacustomcode.named_credential.base import NamedCredential
from datacustomcode.named_credential.spark_base import SparkNamedCredential

_N = TypeVar("_N", bound=NamedCredential)
_S = TypeVar("_S", bound=SparkNamedCredential)


class NamedCredentialObjectConfig(BaseObjectConfig, Generic[_N]):
    type_to_create: ClassVar[Type[NamedCredential]] = NamedCredential  # type: ignore[type-abstract]

    def to_object(self) -> NamedCredential:
        type_ = self.type_to_create.subclass_from_config_name(self.type_config_name)
        return type_(**self.options)


class NamedCredentialConfig(BaseConfig):
    named_credential_config: Union[
        NamedCredentialObjectConfig[NamedCredential], None
    ] = None

    def update(self, other: "NamedCredentialConfig") -> "NamedCredentialConfig":
        def merge(
            config_a: Union[NamedCredentialObjectConfig, None],
            config_b: Union[NamedCredentialObjectConfig, None],
        ) -> Union[NamedCredentialObjectConfig, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.named_credential_config = merge(
            self.named_credential_config, other.named_credential_config
        )
        return self


class SparkNamedCredentialObjectConfig(BaseObjectConfig, Generic[_S]):
    type_to_create: ClassVar[Type[SparkNamedCredential]] = SparkNamedCredential  # type: ignore[type-abstract]

    def to_object(self) -> SparkNamedCredential:
        type_ = self.type_to_create.subclass_from_config_name(self.type_config_name)
        return type_(**self.options)


class SparkNamedCredentialConfig(BaseConfig):
    spark_named_credential_config: Union[
        SparkNamedCredentialObjectConfig[SparkNamedCredential], None
    ] = None

    def update(
        self, other: "SparkNamedCredentialConfig"
    ) -> "SparkNamedCredentialConfig":
        def merge(
            config_a: Union[SparkNamedCredentialObjectConfig, None],
            config_b: Union[SparkNamedCredentialObjectConfig, None],
        ) -> Union[SparkNamedCredentialObjectConfig, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.spark_named_credential_config = merge(
            self.spark_named_credential_config, other.spark_named_credential_config
        )
        return self


# Global Named Credential config instance
named_credential_config = NamedCredentialConfig()
named_credential_config.load(default_config_file())


# Global Spark Named Credential config instance
spark_named_credential_config = SparkNamedCredentialConfig()
spark_named_credential_config.load(default_config_file())
