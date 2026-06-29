from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.einstein_predictions import (
    DefaultSparkEinsteinPredictions,
    EinsteinPredictionsCallError,
)
from datacustomcode.einstein_predictions.spark_default import (
    _STATUS_ERROR,
    _STATUS_SUCCESS,
    _build_underlying_predictions,
    _feature_to_prediction_column,
    _invoke_predictions,
    _invoke_predictions_as_struct,
)
from datacustomcode.einstein_predictions.types import PredictionResponse, PredictionType


def _success_response(data: dict | None = None) -> PredictionResponse:
    return PredictionResponse(
        prediction_type=PredictionType.REGRESSION,
        status_code=200,
        data=data if data is not None else {"results": [{"predictedValue": 42.5}]},
    )


def _error_response(
    status_code: int = 500, error_code: str = "INTERNAL_ERROR"
) -> PredictionResponse:
    return PredictionResponse(
        prediction_type=PredictionType.REGRESSION,
        status_code=status_code,
        data={"errorCode": error_code},
    )


class TestDefaultSparkEinsteinPredictionsConstruction:
    """Construction wires an underlying ``EinsteinPredictions``."""

    def test_uses_injected_predictions_when_provided(self):
        injected = MagicMock()
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=injected)
        assert predictions._einstein_predictions is injected

    @patch(
        "datacustomcode.einstein_predictions.spark_default."
        "_build_underlying_predictions"
    )
    def test_falls_back_to_config_when_none_injected(self, mock_build):
        config_built = MagicMock()
        mock_build.return_value = config_built

        predictions = DefaultSparkEinsteinPredictions()

        mock_build.assert_called_once_with()
        assert predictions._einstein_predictions is config_built


class TestBuildUnderlyingPredictions:
    """``_build_underlying_predictions`` resolves the config-defined resource."""

    def test_returns_object_from_config(self):
        with patch(
            "datacustomcode.einstein_predictions_config.einstein_predictions_config"
        ) as mock_obj_config:
            mock_predictions = MagicMock()
            mock_obj_config.einstein_predictions_config.to_object.return_value = (
                mock_predictions
            )

            assert _build_underlying_predictions() is mock_predictions
            mock_obj_config.einstein_predictions_config.to_object.assert_called_once_with()

    def test_raises_when_config_missing(self):
        with patch(
            "datacustomcode.einstein_predictions_config.einstein_predictions_config"
        ) as mock_obj_config:
            mock_obj_config.einstein_predictions_config = None
            with pytest.raises(RuntimeError, match="einstein_predictions_config"):
                _build_underlying_predictions()


class TestFeatureToPredictionColumn:
    """Python scalar types map to the matching prediction column value type."""

    def test_bool_maps_to_boolean_values(self):
        col = _feature_to_prediction_column("flag", True)
        assert col.boolean_values == [True]
        assert col.double_values is None
        assert col.string_values is None

    def test_int_maps_to_double_values(self):
        col = _feature_to_prediction_column("count", 3)
        assert col.double_values == [3.0]

    def test_float_maps_to_double_values(self):
        col = _feature_to_prediction_column("amount", 2.5)
        assert col.double_values == [2.5]

    def test_str_maps_to_string_values(self):
        col = _feature_to_prediction_column("name", "Ada")
        assert col.string_values == ["Ada"]


class TestDefaultSparkEinsteinPredictionsPredict:

    def test_forwards_model_and_features(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _success_response({"results": [1]})
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        result = predictions.einstein_predict(
            "model1", PredictionType.REGRESSION, {"beds": 3, "city": "SF"}
        )

        assert result == {"results": [1]}
        sent = mock_inner.predict.call_args.args[0]
        assert sent.model_api_name == "model1"
        assert sent.prediction_type == PredictionType.REGRESSION
        column_names = {c.column_name for c in sent.prediction_columns}
        assert column_names == {"beds", "city"}

    def test_forwards_settings(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _success_response()
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        predictions.einstein_predict(
            "model1",
            PredictionType.CLUSTERING,
            {"x": 1.0},
            settings={"maxTopContributors": 20},
        )

        sent = mock_inner.predict.call_args.args[0]
        assert sent.settings == {"maxTopContributors": 20}


class TestDefaultSparkEinsteinPredictionsPredictCol:

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_dict_features_built_into_struct_and_wrapped_in_udf(
        self, mock_struct, mock_udf
    ):
        sentinel_struct_col = MagicMock(name="struct_col")
        mock_struct.return_value = sentinel_struct_col
        sentinel_udf = MagicMock(name="udf")
        sentinel_applied = MagicMock(name="udf_applied")
        sentinel_udf.return_value = sentinel_applied
        mock_udf.return_value = sentinel_udf

        mock_inner = MagicMock()
        mock_inner.predict.return_value = _success_response({"results": [7]})
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        beds_col, baths_col = MagicMock(name="beds"), MagicMock(name="baths")
        beds_aliased, baths_aliased = (
            MagicMock(name="beds_aliased"),
            MagicMock(name="baths_aliased"),
        )
        beds_col.alias.return_value = beds_aliased
        baths_col.alias.return_value = baths_aliased

        result = predictions.einstein_predict_col(
            "model1",
            PredictionType.REGRESSION,
            {"beds": beds_col, "baths": baths_col},
        )

        beds_col.alias.assert_called_once_with("beds")
        baths_col.alias.assert_called_once_with("baths")
        mock_struct.assert_called_once_with(beds_aliased, baths_aliased)
        mock_udf.assert_called_once()
        sentinel_udf.assert_called_once_with(sentinel_struct_col)
        assert result is sentinel_applied

        udf_fn = mock_udf.call_args.args[0]
        row = MagicMock()
        row.asDict.return_value = {"beds": 3.0, "baths": 2.0}
        out = udf_fn(row)

        assert out["status"] == _STATUS_SUCCESS
        assert json.loads(out["response"]) == {"results": [7]}
        assert out["error_code"] is None
        sent = mock_inner.predict.call_args.args[0]
        assert sent.model_api_name == "model1"

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_udf_returns_error_struct_for_null_row(self, mock_struct, mock_udf):
        mock_struct.return_value = MagicMock()
        mock_udf.return_value = MagicMock()
        mock_inner = MagicMock()
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        predictions.einstein_predict_col(
            "model1", PredictionType.REGRESSION, {"beds": MagicMock()}
        )

        udf_fn = mock_udf.call_args.args[0]
        out = udf_fn(None)
        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert "null" in out["error_message"].lower()
        mock_inner.predict.assert_not_called()

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.struct")
    def test_udf_returns_error_struct_on_http_error(self, mock_struct, mock_udf):
        """Per-row errors are returned as ``status="ERROR"`` structs so one bad
        row does not abort the Spark job."""
        mock_struct.return_value = MagicMock()
        mock_udf.return_value = MagicMock()
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        predictions.einstein_predict_col(
            "model1", PredictionType.REGRESSION, {"beds": MagicMock()}
        )

        udf_fn = mock_udf.call_args.args[0]
        row = MagicMock()
        row.asDict.return_value = {"beds": 3.0}
        out = udf_fn(row)

        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert out["error_code"] == "UNAVAILABLE"
        assert out["error_message"] is not None


class TestInvokePredictions:

    def test_returns_response_data(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _success_response({"results": [1]})

        out = _invoke_predictions(
            mock_inner, "model", PredictionType.REGRESSION, {"x": 1.0}, None
        )
        assert out == {"results": [1]}

    def test_raises_call_error_on_error_response(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )

        with pytest.raises(EinsteinPredictionsCallError) as excinfo:
            _invoke_predictions(
                mock_inner, "model", PredictionType.REGRESSION, {"x": 1.0}, None
            )

        assert excinfo.value.status == 503
        assert excinfo.value.error_code == "UNAVAILABLE"
        assert "503" in str(excinfo.value)
        assert "UNAVAILABLE" in str(excinfo.value)


class TestInvokePredictionsAsStruct:
    """Non-raising variant used by the per-row UDF. Both SUCCESS and ERROR
    cases land in the same struct shape so callers can select fields
    uniformly."""

    def test_success_returns_success_struct(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _success_response({"results": [9]})

        out = _invoke_predictions_as_struct(
            mock_inner, "model", PredictionType.REGRESSION, {"x": 1.0}, None
        )

        assert out["status"] == _STATUS_SUCCESS
        assert json.loads(out["response"]) == {"results": [9]}
        assert out["error_code"] is None
        assert out["error_message"] is None

    def test_error_returns_error_struct_without_raising(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _error_response(
            status_code=503, error_code="UNAVAILABLE"
        )

        out = _invoke_predictions_as_struct(
            mock_inner, "model", PredictionType.REGRESSION, {"x": 1.0}, None
        )

        assert out["status"] == _STATUS_ERROR
        assert out["response"] is None
        assert out["error_code"] == "UNAVAILABLE"
        assert out["error_message"] is not None


class TestDefaultSparkEinsteinPredictionsErrorHandling:
    """The scalar einstein_predict path raises when the underlying resource
    errors."""

    def test_raises_on_error_response(self):
        mock_inner = MagicMock()
        mock_inner.predict.return_value = _error_response(
            status_code=429, error_code="RATE_LIMITED"
        )
        predictions = DefaultSparkEinsteinPredictions(einstein_predictions=mock_inner)

        with pytest.raises(EinsteinPredictionsCallError) as excinfo:
            predictions.einstein_predict(
                "model1", PredictionType.REGRESSION, {"x": 1.0}
            )

        assert excinfo.value.status == 429
        assert excinfo.value.error_code == "RATE_LIMITED"
