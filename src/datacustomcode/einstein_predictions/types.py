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

from enum import Enum, unique
from typing import (
    Any,
    Dict,
    Literal,
    Optional,
)

from pydantic import (
    BaseModel,
    Field,
    model_validator,
)


@unique
class PredictionType(Enum):
    REGRESSION = 1
    CLUSTERING = 2
    CLASSIFICATION = 3
    MULTI_OUTCOME = 4
    BINARY_CLASSIFICATION = 5


class PredictionColumn(BaseModel):
    column_name: str = Field(min_length=1, description="Column name")
    string_values: Optional[list[str]] = Field(
        default=None, min_length=1, description="Column string values"
    )
    double_values: Optional[list[float]] = Field(
        default=None, min_length=1, description="Column double values"
    )
    boolean_values: Optional[list[bool]] = Field(
        default=None, min_length=1, description="Column boolean values"
    )
    date_values: Optional[list[str]] = Field(
        default=None, min_length=1, description="Column date values"
    )
    datetime_values: Optional[list[str]] = Field(
        default=None, min_length=1, description="Column datetime values"
    )

    @model_validator(mode="after")
    def validate_exactly_one_value_type(self):
        set_count = sum(
            [
                self.string_values is not None,
                self.double_values is not None,
                self.boolean_values is not None,
                self.date_values is not None,
                self.datetime_values is not None,
            ]
        )

        if set_count != 1:
            raise ValueError("Exactly one value type must be set")

        return self


class PredictionColumBuilder:
    def __init__(self) -> None:
        self._column_name: Optional[str] = None
        self._string_values: Optional[list[str]] = None
        self._double_values: Optional[list[float]] = None
        self._boolean_values: Optional[list[bool]] = None
        self._date_values: Optional[list[str]] = None
        self._datetime_values: Optional[list[str]] = None

    def set_column_name(self, column_name: str) -> "PredictionColumBuilder":
        self._column_name = column_name
        return self

    def set_string_values(self, string_values: list[str]) -> "PredictionColumBuilder":
        self._string_values = string_values
        return self

    def set_double_values(self, double_values: list[float]) -> "PredictionColumBuilder":
        self._double_values = double_values
        return self

    def set_boolean_values(
        self, boolean_values: list[bool]
    ) -> "PredictionColumBuilder":
        self._boolean_values = boolean_values
        return self

    def set_date_values(self, date_values: list[str]) -> "PredictionColumBuilder":
        self._date_values = date_values
        return self

    def set_datetime_values(
        self, datetime_values: list[str]
    ) -> "PredictionColumBuilder":
        self._datetime_values = datetime_values
        return self

    def build(self) -> PredictionColumn:
        return PredictionColumn(
            column_name=self._column_name,
            string_values=self._string_values,
            double_values=self._double_values,
            boolean_values=self._boolean_values,
            date_values=self._date_values,
            datetime_values=self._datetime_values,
        )


class PredictionRequest(BaseModel):
    version: Literal["v1"] = Field(
        default="v1", description="API version, must be 'v1'"
    )
    prediction_type: PredictionType = Field(description="Prediction type")
    model_api_name: str = Field(
        min_length=1, description="API name of the model to use"
    )
    prediction_columns: list[PredictionColumn] = Field(
        min_length=1, description="List of prediction columns"
    )
    settings: Optional[Dict[str, Any]] = Field(
        default=None, description="Settings for the prediction request"
    )


class PredictionRequestBuilder:
    def __init__(self) -> None:
        self._prediction_type: Optional[PredictionType] = None
        self._model_api_name: Optional[str] = None
        self._prediction_columns: list[PredictionColumn] = []
        self._settings: Optional[Dict[str, Any]] = None

    def set_prediction_type(
        self, prediction_type: PredictionType
    ) -> "PredictionRequestBuilder":
        self._prediction_type = prediction_type
        return self

    def set_model_api_name(self, model_api_name: str) -> "PredictionRequestBuilder":
        self._model_api_name = model_api_name
        return self

    def set_prediction_columns(
        self, prediction_columns: list[PredictionColumn]
    ) -> "PredictionRequestBuilder":
        self._prediction_columns = prediction_columns
        return self

    def set_settings(self, settings: Dict[str, Any]):
        self._settings = settings
        return self

    def build(self) -> PredictionRequest:
        return PredictionRequest(
            prediction_type=self._prediction_type,
            model_api_name=self._model_api_name,
            prediction_columns=self._prediction_columns,
            settings=self._settings,
        )


class PredictionResponse(BaseModel):
    version: Literal["v1"] = Field(default="v1", description="API version")
    prediction_type: PredictionType = Field(description="Prediction type")
    status_code: int = Field(description="HTTP status code")
    data: Optional[Dict[str, Any]] = Field(default=None, description="Response data")

    @property
    def is_success(self) -> bool:
        return self.status_code == 200
