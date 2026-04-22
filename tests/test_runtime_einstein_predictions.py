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

from datacustomcode.einstein_predictions.base import EinsteinPredictions
from datacustomcode.einstein_predictions.types import (
    PredictionColumn,
    PredictionRequest,
    PredictionResponse,
    PredictionType,
)
from datacustomcode.einstein_predictions_config import EinsteinPredictionsObjectConfig


class TestCustomEinsteinPredictionsImplementation:
    """Test that other implementations are supported"""

    def test_custom_implementation_is_discoverable(self):
        class CustomEinsteinPredictions(EinsteinPredictions):
            CONFIG_NAME = "CustomEinsteinPredictions"

            def __init__(self, custom_param: str = "default", **kwargs):
                super().__init__(**kwargs)
                self.custom_param = custom_param

            def predict(self, request: PredictionRequest) -> PredictionResponse:
                return PredictionResponse(
                    version="v1",
                    prediction_type=request.prediction_type,
                    status_code=200,
                    data={"results": [{"predictedValue": 1}]},
                )

        available_names = EinsteinPredictions.available_config_names()
        assert "CustomEinsteinPredictions" in available_names

        cls = EinsteinPredictions.subclass_from_config_name("CustomEinsteinPredictions")
        assert cls == CustomEinsteinPredictions

        # Verify we can create via config
        ep_config = EinsteinPredictionsObjectConfig(
            type_config_name="CustomEinsteinPredictions",
            options={"custom_param": "my_value"},
        )
        instance = ep_config.to_object()
        assert isinstance(instance, CustomEinsteinPredictions)
        assert instance.custom_param == "my_value"

        request = PredictionRequest(
            prediction_type=PredictionType.REGRESSION,
            model_api_name="test",
            prediction_columns=[
                PredictionColumn(column_name="col1", double_values=[1.0])
            ],
        )
        response = instance.predict(request)
        assert response.is_success is True
        assert response.data["results"] == [{"predictedValue": 1}]
