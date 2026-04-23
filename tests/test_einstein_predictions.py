from pydantic import ValidationError
import pytest

from datacustomcode.einstein_predictions.types import (
    PredictionColumBuilder,
    PredictionColumn,
    PredictionRequest,
    PredictionRequestBuilder,
    PredictionResponse,
    PredictionType,
)


class TestPredictionColumnValidation:
    def test_string_values_only(self):
        column = PredictionColumn(column_name="test_col", string_values=["a", "b", "c"])
        assert column.column_name == "test_col"
        assert column.string_values == ["a", "b", "c"]
        assert column.double_values is None
        assert column.boolean_values is None
        assert column.date_values is None
        assert column.datetime_values is None

    def test_double_values_only(self):
        column = PredictionColumn(column_name="test_col", double_values=[1.0, 2.5, 3.7])
        assert column.double_values == [1.0, 2.5, 3.7]
        assert column.string_values is None
        assert column.boolean_values is None
        assert column.date_values is None
        assert column.datetime_values is None

    def test_boolean_values_only(self):
        column = PredictionColumn(
            column_name="test_col", boolean_values=[True, False, True]
        )
        assert column.boolean_values == [True, False, True]
        assert column.string_values is None
        assert column.double_values is None
        assert column.date_values is None
        assert column.datetime_values is None

    def test_date_values_only(self):
        column = PredictionColumn(
            column_name="test_col", date_values=["2024-01-01", "2024-01-02"]
        )
        assert column.date_values == ["2024-01-01", "2024-01-02"]
        assert column.string_values is None
        assert column.double_values is None
        assert column.boolean_values is None
        assert column.datetime_values is None

    def test_datetime_values_only(self):
        column = PredictionColumn(
            column_name="test_col",
            datetime_values=["2024-01-01T12:00:00", "2024-01-02T13:00:00"],
        )
        assert column.datetime_values == ["2024-01-01T12:00:00", "2024-01-02T13:00:00"]
        assert column.string_values is None
        assert column.double_values is None
        assert column.boolean_values is None
        assert column.date_values is None

    def test_no_column_name_raises_error(self):
        with pytest.raises(ValidationError) as exc_info:
            PredictionColumn(
                column_name="", string_values=["a", "b"], double_values=[1.0, 2.0]
            )

        assert str(exc_info.value) is not None

    def test_no_values_raises_error(self):
        with pytest.raises(ValidationError) as exc_info:
            PredictionColumn(column_name="test_col")

        assert str(exc_info.value) is not None

    def test_string_and_double_raises_error(self):
        with pytest.raises(ValidationError) as exc_info:
            PredictionColumn(
                column_name="test_col",
                string_values=["a", "b"],
                double_values=[1.0, 2.0],
            )

        assert str(exc_info.value) is not None

    def test_empty_values_raises_error(self):
        with pytest.raises(ValidationError) as exc_info:
            PredictionColumn(column_name="test_col", string_values=[])

        assert str(exc_info.value) is not None


class TestPredictionColumnBuilder:
    def test_builder_with_string_values(self):
        column = (
            PredictionColumBuilder()
            .set_column_name("test_col")
            .set_string_values(["a", "b"])
            .build()
        )

        assert column.column_name == "test_col"
        assert column.string_values == ["a", "b"]


class TestPredictionRequest:
    def test_request_with_multiple_columns(self):
        request = PredictionRequest(
            prediction_type=PredictionType.CLASSIFICATION,
            model_api_name="classifier",
            prediction_columns=[
                PredictionColumn(column_name="col1", string_values=["a"]),
                PredictionColumn(column_name="col2", double_values=[1.0]),
                PredictionColumn(column_name="col3", boolean_values=[True]),
            ],
        )

        assert len(request.prediction_columns) == 3

    def test_request_requires_model_api_name(self):
        with pytest.raises(ValidationError):
            PredictionRequest(
                prediction_type=PredictionType.REGRESSION,
                model_api_name="",
                prediction_columns=[
                    PredictionColumn(column_name="col1", double_values=[1.0])
                ],
            )

    def test_request_requires_prediction_columns(self):
        with pytest.raises(ValidationError):
            PredictionRequest(
                prediction_type=PredictionType.REGRESSION,
                model_api_name="model",
                prediction_columns=[],
            )


class TestPredictionRequestBuilder:
    def test_builder_creates_valid_request(self):
        request = (
            PredictionRequestBuilder()
            .set_prediction_type(PredictionType.CLUSTERING)
            .set_model_api_name("cluster_model")
            .set_prediction_columns(
                [PredictionColumn(column_name="test_col", double_values=[1.0])]
            )
            .set_settings({"maxTopContributors": 20})
            .build()
        )

        assert request.prediction_type == PredictionType.CLUSTERING
        assert request.model_api_name == "cluster_model"
        assert len(request.prediction_columns) == 1
        assert request.settings == {"maxTopContributors": 20}


class TestPredictionResponse:
    def test_successful_response(self):
        response = PredictionResponse(
            version="v1",
            prediction_type=PredictionType.REGRESSION,
            status_code=200,
            data={"results": [{"prediction": {"value": 42.5}}]},
        )

        assert response.is_success
        assert response.status_code == 200
        assert response.data is not None

    def test_failed_response(self):
        response = PredictionResponse(
            version="v1",
            prediction_type=PredictionType.REGRESSION,
            status_code=500,
            data={"error": "Internal server error"},
        )

        assert not response.is_success
        assert response.status_code == 500
