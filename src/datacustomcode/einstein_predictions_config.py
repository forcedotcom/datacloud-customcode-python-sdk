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
from datacustomcode.einstein_platform_config import CredentialsObjectConfig
from datacustomcode.einstein_predictions.base import EinsteinPredictions
from datacustomcode.einstein_predictions.spark_base import SparkEinsteinPredictions

_E = TypeVar("_E", bound=EinsteinPredictions)
_S = TypeVar("_S", bound=SparkEinsteinPredictions)


class EinsteinPredictionsObjectConfig(CredentialsObjectConfig[_E], Generic[_E]):
    type_to_create: ClassVar[Type[EinsteinPredictions]] = EinsteinPredictions  # type: ignore[type-abstract]


class EinsteinPredictionsConfig(BaseConfig):
    einstein_predictions_config: Union[
        EinsteinPredictionsObjectConfig[EinsteinPredictions], None
    ] = None

    def update(self, other: "EinsteinPredictionsConfig") -> "EinsteinPredictionsConfig":
        def merge(
            config_a: Union[EinsteinPredictionsObjectConfig, None],
            config_b: Union[EinsteinPredictionsObjectConfig, None],
        ) -> Union[EinsteinPredictionsObjectConfig, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.einstein_predictions_config = merge(
            self.einstein_predictions_config, other.einstein_predictions_config
        )
        return self


class SparkEinsteinPredictionsObjectConfig(BaseObjectConfig, Generic[_S]):
    type_to_create: ClassVar[Type[SparkEinsteinPredictions]] = SparkEinsteinPredictions  # type: ignore[type-abstract]

    def to_object(self) -> SparkEinsteinPredictions:
        type_ = self.type_to_create.subclass_from_config_name(self.type_config_name)
        return type_(**self.options)


class SparkEinsteinPredictionsConfig(BaseConfig):
    spark_einstein_predictions_config: Union[
        SparkEinsteinPredictionsObjectConfig[SparkEinsteinPredictions], None
    ] = None

    def update(
        self, other: "SparkEinsteinPredictionsConfig"
    ) -> "SparkEinsteinPredictionsConfig":
        def merge(
            config_a: Union[SparkEinsteinPredictionsObjectConfig, None],
            config_b: Union[SparkEinsteinPredictionsObjectConfig, None],
        ) -> Union[SparkEinsteinPredictionsObjectConfig, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.spark_einstein_predictions_config = merge(
            self.spark_einstein_predictions_config,
            other.spark_einstein_predictions_config,
        )
        return self


# Global Einstein Predictions config instance
einstein_predictions_config = EinsteinPredictionsConfig()
einstein_predictions_config.load(default_config_file())


# Global Spark Einstein Predictions config instance
spark_einstein_predictions_config = SparkEinsteinPredictionsConfig()
spark_einstein_predictions_config.load(default_config_file())
