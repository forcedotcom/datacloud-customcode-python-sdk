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

import json
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

from datacustomcode.einstein_predictions.spark_base import SparkEinsteinPredictions
from datacustomcode.einstein_predictions.types import (
    PredictionColumn,
    PredictionRequest,
    PredictionType,
)

if TYPE_CHECKING:
    from pyspark.sql import Column

    from datacustomcode.einstein_predictions.base import EinsteinPredictions
    from datacustomcode.einstein_predictions.types import PredictionResponse


_STATUS_SUCCESS = "SUCCESS"
_STATUS_ERROR = "ERROR"


class DefaultSparkEinsteinPredictions(SparkEinsteinPredictions):

    CONFIG_NAME = "DefaultSparkEinsteinPredictions"

    def __init__(
        self,
        einstein_predictions: Optional["EinsteinPredictions"] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if einstein_predictions is None:
            einstein_predictions = _build_underlying_predictions()
        self._einstein_predictions: "EinsteinPredictions" = einstein_predictions

    def einstein_predict(
        self,
        model_api_name: str,
        prediction_type: PredictionType,
        features: Dict[str, Any],
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return _invoke_predictions(
            self._einstein_predictions,
            model_api_name,
            prediction_type,
            features,
            settings,
        )

    def einstein_predict_col(
        self,
        model_api_name: str,
        prediction_type: PredictionType,
        features: Dict[str, "Column"],
        settings: Optional[Dict[str, Any]] = None,
    ) -> "Column":
        """Build a per-row UDF that returns a struct ``{status, response,
        error_code, error_message}`` so per-row failures do not abort the
        Spark job. Callers select the field they want, e.g.
        ``einstein_predict_col(...)["response"]``. ``response`` carries the
        prediction response payload serialized as a JSON string.
        """
        from pyspark.sql.functions import struct, udf
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
        )

        feature_names = list(features.keys())
        values_col = struct(*[features[name].alias(name) for name in feature_names])

        predictions = self._einstein_predictions
        result_schema = StructType(
            [
                StructField("status", StringType(), True),
                StructField("response", StringType(), True),
                StructField("error_code", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        def _predict(values_row: Any) -> Dict[str, Optional[str]]:
            if values_row is None:
                return {
                    "status": _STATUS_ERROR,
                    "response": None,
                    "error_code": None,
                    "error_message": "features column was null for this row",
                }
            row_features = (
                values_row.asDict()
                if hasattr(values_row, "asDict")
                else dict(values_row)
            )
            return _invoke_predictions_as_struct(
                predictions,
                model_api_name,
                prediction_type,
                row_features,
                settings,
            )

        return udf(_predict, result_schema)(values_col)


def _build_underlying_predictions() -> "EinsteinPredictions":
    from datacustomcode.einstein_predictions_config import einstein_predictions_config

    cfg = einstein_predictions_config.einstein_predictions_config
    if cfg is None:
        raise RuntimeError(
            "einstein_predictions_config is not configured. Add an "
            "'einstein_predictions_config' section to config.yaml."
        )
    return cfg.to_object()


def _feature_to_prediction_column(name: str, value: Any) -> PredictionColumn:
    """Wrap a single scalar feature value into a one-element prediction column.

    The value's Python type selects the prediction column value type. ``bool``
    is checked before ``int``/``float`` because ``bool`` is a subclass of
    ``int`` in Python.
    """
    if isinstance(value, bool):
        return PredictionColumn(column_name=name, boolean_values=[value])
    if isinstance(value, (int, float)):
        return PredictionColumn(column_name=name, double_values=[float(value)])
    return PredictionColumn(column_name=name, string_values=[str(value)])


def _build_request(
    model_api_name: str,
    prediction_type: PredictionType,
    features: Dict[str, Any],
    settings: Optional[Dict[str, Any]],
) -> PredictionRequest:
    prediction_columns = [
        _feature_to_prediction_column(name, value)
        for name, value in features.items()
    ]
    return PredictionRequest(
        prediction_type=prediction_type,
        model_api_name=model_api_name,
        prediction_columns=prediction_columns,
        settings=settings,
    )


def _call_predictions(
    predictions: "EinsteinPredictions",
    model_api_name: str,
    prediction_type: PredictionType,
    features: Dict[str, Any],
    settings: Optional[Dict[str, Any]],
) -> "PredictionResponse":
    """Build the request and dispatch it to the underlying predictions resource."""
    request = _build_request(model_api_name, prediction_type, features, settings)
    return predictions.predict(request)


def _invoke_predictions(
    predictions: "EinsteinPredictions",
    model_api_name: str,
    prediction_type: PredictionType,
    features: Dict[str, Any],
    settings: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    from datacustomcode.einstein_predictions.errors import EinsteinPredictionsCallError

    response = _call_predictions(
        predictions, model_api_name, prediction_type, features, settings
    )
    if not response.is_success:
        error_code = _extract_error_code(response)
        raise EinsteinPredictionsCallError(
            f"Einstein Predictions call failed: "
            f"status_code={response.status_code}, "
            f"error_code={error_code!r}, message={response.data!r}",
            status=response.status_code,
            error_code=error_code,
            error_message=str(response.data) if response.data else None,
        )
    return response.data or {}


def _invoke_predictions_as_struct(
    predictions: "EinsteinPredictions",
    model_api_name: str,
    prediction_type: PredictionType,
    features: Dict[str, Any],
    settings: Optional[Dict[str, Any]],
) -> Dict[str, Optional[str]]:
    response = _call_predictions(
        predictions, model_api_name, prediction_type, features, settings
    )
    if not response.is_success:
        return {
            "status": _STATUS_ERROR,
            "response": None,
            "error_code": _extract_error_code(response),
            "error_message": str(response.data) if response.data else None,
        }
    return {
        "status": _STATUS_SUCCESS,
        "response": json.dumps(response.data) if response.data is not None else None,
        "error_code": None,
        "error_message": None,
    }


def _extract_error_code(response: "PredictionResponse") -> Optional[str]:
    if response.data:
        error_code = response.data.get("errorCode")
        if error_code is not None:
            return str(error_code)
    return None
