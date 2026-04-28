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

from datacustomcode.common_config import BaseConfig, default_config_file
from datacustomcode.einstein_platform_config import CredentialsObjectConfig
from datacustomcode.einstein_predictions.base import EinsteinPredictions

_E = TypeVar("_E", bound=EinsteinPredictions)


class EinsteinPredictionsObjectConfig(CredentialsObjectConfig, Generic[_E]):
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


# Global Einstein Predictions config instance
einstein_predictions_config = EinsteinPredictionsConfig()
einstein_predictions_config.load(default_config_file())
