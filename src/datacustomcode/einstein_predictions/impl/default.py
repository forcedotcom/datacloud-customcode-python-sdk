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
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
)

from loguru import logger
import requests

from datacustomcode.einstein_predictions.base import EinsteinPredictions
from datacustomcode.einstein_predictions.types import (
    PredictionRequest,
    PredictionResponse,
    PredictionType,
)
from datacustomcode.token_provider import (
    CredentialsTokenProvider,
    SFCLITokenProvider,
    TokenProvider,
)


class DefaultEinsteinPredictions(EinsteinPredictions):
    CONFIG_NAME = "DefaultEinsteinPredictions"
    EINSTEIN_PLATFORM_URL = "https://api.salesforce.com/einstein/platform/v1"

    ENDPOINT_MAP: ClassVar[dict[PredictionType, str]] = {
        PredictionType.REGRESSION: "regression",
        PredictionType.CLUSTERING: "clustering",
        PredictionType.CLASSIFICATION: "classification",
        PredictionType.BINARY_CLASSIFICATION: "binary-classification",
        PredictionType.MULTI_OUTCOME: "multi-outcome",
    }

    def __init__(
        self,
        credentials_profile: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if sf_cli_org:
            self._token_provider: TokenProvider = SFCLITokenProvider(sf_cli_org)
            logger.debug(f"Using SF CLI token provider for org: {sf_cli_org}")
        else:
            profile = credentials_profile or "default"
            self._token_provider = CredentialsTokenProvider(profile)
            logger.debug(f"Using credentials token provider with profile: {profile}")

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        """Make a prediction request to the Einstein Predictions API"""
        token_response = self._token_provider.get_token()
        access_token = token_response.access_token

        endpoint = self.ENDPOINT_MAP.get(request.prediction_type)
        if not endpoint:
            raise RuntimeError(
                f"Unknown prediction type: {request.prediction_type}. "
                f"Valid types: {list(self.ENDPOINT_MAP.keys())}"
            )

        api_url = (
            f"{self.EINSTEIN_PLATFORM_URL}/models/"
            f"{request.model_api_name}/{endpoint}"
        )

        prediction_columns: List[Dict[str, Any]] = []
        for col in request.prediction_columns:
            col_data: Dict[str, Any] = {"columnName": col.column_name}
            if col.string_values:
                col_data["stringValues"] = col.string_values
            if col.double_values:
                col_data["doubleValues"] = col.double_values
            if col.boolean_values:
                col_data["booleanValues"] = col.boolean_values
            if col.date_values:
                col_data["dateValues"] = col.date_values
            if col.datetime_values:
                col_data["datetimeValues"] = col.datetime_values
            prediction_columns.append(col_data)

        payload: Dict[str, Any] = {"predictionColumns": prediction_columns}

        if request.settings:
            payload["settings"] = request.settings

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-sfdc-app-context": "EinsteinGPT",
            "x-client-feature-id": "ai-platform-models-connected-app",
        }

        logger.debug(f"Making Einstein prediction request to: {api_url}")
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
            if not response.ok and not response.text:
                error_msg = (
                    f"Einstein Prediction request failed: {api_url} - "
                    f"{response.status_code} {response.reason}. "
                    "If your code uses Einstein APIs, make sure you have "
                    'configured the SDK to use "client_credentials" auth type. '
                    "Refer to https://developer.salesforce.com/docs/ai/agentforce/"
                    "guide/agent-api-get-started.html#create-a-salesforce-app "
                    "to create your external client app."
                )
                logger.error(error_msg)
        except requests.exceptions.RequestException as e:
            logger.error(f"Prediction API request failed: {api_url} {e}")
            raise RuntimeError(f"Prediction API request failed: {e}") from e

        response_data: Dict[str, Any] = {}
        if response.content:
            try:
                response_data = response.json()
            except ValueError:
                logger.warning("Failed to parse response as JSON")
                response_data = {"raw_response": response.text}

        return PredictionResponse(
            version="v1",
            prediction_type=request.prediction_type,
            status_code=response.status_code,
            data=response_data,
        )
