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

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

from datacustomcode.einstein_predictions.types import PredictionType
from datacustomcode.mixin import UserExtendableNamedConfigMixin

if TYPE_CHECKING:
    from pyspark.sql import Column


class SparkEinsteinPredictions(ABC, UserExtendableNamedConfigMixin):
    CONFIG_NAME: str

    def __init__(self, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def einstein_predict(
        self,
        model_api_name: str,
        prediction_type: PredictionType,
        features: Dict[str, Any],
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Issue a one-shot Einstein prediction and return the response data.

        ``features`` maps each model feature column name to a single scalar
        value (``str``/``float``/``bool``). The value is wrapped into a
        single-element prediction column of the appropriate type.
        """

    @abstractmethod
    def einstein_predict_col(
        self,
        model_api_name: str,
        prediction_type: PredictionType,
        features: Dict[str, "Column"],
        settings: Optional[Dict[str, Any]] = None,
    ) -> "Column":
        """Build a Spark ``Column`` that invokes Einstein predict per row and
        yields a struct ``{status, response, error_code, error_message}``.

        ``features`` maps each model feature column name to a Spark ``Column``
        supplying that feature's per-row value. Select an individual field,
        e.g. ``einstein_predict_col(...)["response"]``. ``response`` holds the
        prediction response payload as a JSON string. Returning a struct means
        a single failing row leaves the rest of the DataFrame intact — callers
        can inspect ``status`` / ``error_code`` per row instead of having the
        Spark job abort.
        """
