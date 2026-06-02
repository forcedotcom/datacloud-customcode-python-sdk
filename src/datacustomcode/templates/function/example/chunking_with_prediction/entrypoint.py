#!/usr/bin/env python3
# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2

"""
Housing Sale Price Prediction with Einstein Regression

This example uses Einstein regression model to predict housing sale prices
based on property features like Year_Built__c.

Model: YH_Regression_Python_Predicted_SalePrice_CM_12l_ATC937af934
Type: Regression
Input: Year_Built__c (numeric)
Output: Predicted_SalePrice
"""

"""
    You can use your AI models configured in Salesforce to make predictions.

    For testing locally before deploying your code to Data Cloud (datacustomcode run),
    first configure an external client app before using LLM functionality, then configure
    the SDK with your client app credentials.
    
    https://developer.salesforce.com/docs/ai/agentforce/guide/agent-api-get-started.html#create-a-salesforce-app
"""

import logging
from typing import (
    Any,
    Dict,
    Optional,
)

from datacustomcode.einstein_predictions.types import (
    PredictionColumBuilder,
    PredictionRequestBuilder,
    PredictionType,
)
from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.chunking import (
    ChunkType,
    SearchIndexChunkingV1Output,
    SearchIndexChunkingV1Request,
    SearchIndexChunkingV1Response,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
PREDICTION_MODEL_NAME = "YH_Regression_Python_Predicted_SalePrice_CM_12l_ATC937af934"


def predict_sale_price(
    features: Dict[str, Any],
    runtime: Runtime,
) -> Optional[float]:
    """Predict housing sale price using Einstein regression model.

    Args:
        features: Extracted housing features (numeric and string)
        runtime: Runtime with prediction client

    Returns:
        Predicted sale price or None if prediction fails
    """
    try:
        # Build prediction columns - handle both numeric and string values
        prediction_columns = []

        for column_name, value in features.items():
            if isinstance(value, str):
                # String values (e.g., Garage_Qual__c)
                column = (
                    PredictionColumBuilder()
                    .set_column_name(column_name)
                    .set_string_values([value])
                    .build()
                )
            elif isinstance(value, (int, float)):
                # Numeric values
                column = (
                    PredictionColumBuilder()
                    .set_column_name(column_name)
                    .set_double_values([float(value)])
                    .build()
                )
            else:
                # Skip unsupported types
                logger.warning(
                    f"Skipping field {column_name} with unsupported type {type(value)}"
                )
                continue

            prediction_columns.append(column)

        # Build regression prediction request
        prediction_request = (
            PredictionRequestBuilder()
            .set_prediction_type(PredictionType.REGRESSION)
            .set_model_api_name(PREDICTION_MODEL_NAME)
            .set_prediction_columns(prediction_columns)
            .build()
        )

        logger.info(f"Model: {PREDICTION_MODEL_NAME}")

        prediction_response = runtime.einstein_predictions.predict(prediction_request)

        logger.info(f"Response status code: {prediction_response.status_code}")
        logger.info(f"Response data: {prediction_response.data}")

        if not prediction_response.is_success:
            logger.error(f"Prediction failed: {prediction_response.data}")
            return None

        # Parse regression response
        if prediction_response.data is None:
            logger.warning("Prediction response data is None")
            return None

        results = prediction_response.data.get("results", [])
        if not results:
            logger.warning("No results in prediction response")
            return None

        first_result = results[0]
        prediction_type = first_result.get("type")

        if prediction_type != "RegressionPredictionSuccess":
            logger.error(f"Unexpected prediction type: {prediction_type}")
            logger.error(f"Full result: {first_result}")
            return None

        prediction_data = first_result.get("prediction", {})
        predicted_value = prediction_data.get("predictedValue")

        if predicted_value is None:
            logger.warning("No predicted value in response")
            return None

        logger.info(f"Predicted sale price: ${predicted_value:,.2f}")

        # Log top contributors (which features influenced the price most)
        top_contributors = prediction_data.get("topContributors", [])
        if top_contributors:
            logger.info(f"Top price contributors: {top_contributors}")

        return float(predicted_value)

    except Exception as e:
        logger.error(f"Prediction failed with error: {e}", exc_info=True)
        return None


def enrich_property_with_price(
    source_dmo_fields: Dict[str, Any],
    runtime: Runtime,
) -> Dict[str, str]:
    """Enrich property data with predicted sale price.

    Args:
        source_dmo_fields: Property features from source DMO
        runtime: Runtime for predictions

    Returns:
        Citations dictionary with predicted price
    """
    citations = {}

    # Copy original fields to citations
    if source_dmo_fields:
        for key, value in source_dmo_fields.items():
            citations[key] = str(value)

    # Get price prediction - pass source_dmo_fields directly as features
    predicted_price = predict_sale_price(source_dmo_fields, runtime)

    if predicted_price is not None:
        citations["predicted_sale_price"] = f"${predicted_price:,.2f}"
        citations["predicted_sale_price_raw"] = str(predicted_price)
        citations["prediction_status"] = "success"
    else:
        citations["predicted_sale_price"] = "N/A"
        citations["prediction_status"] = "failed"

    return citations


def function(
    request: SearchIndexChunkingV1Request, runtime: Runtime
) -> SearchIndexChunkingV1Response:
    """Housing price prediction using Einstein regression.

    Predicts sale prices for properties based on Year_Built__c feature
    and adds predictions to citations for real estate data enrichment.

    Input format:
    {
      "input": [
        {
          "text": "Beautiful 3BR house built in 1990",
          "metadata": {
            "source_dmo_fields": {
              "Year_Built__c": 1990,
            }
          }
        }
      ]
    }

    Output format:
    {
      "output": [
        {
          "text": "Beautiful 3BR house built in 1990",
          "seq_no": 1,
          "citations": {
            "Year_Built__c": "1990",
            "predicted_sale_price": "$350,000.00",
            "predicted_sale_price_raw": "350000.0",
            "prediction_status": "success"
          }
        }
      ]
    }

    Args:
        request: Input properties to enrich
        runtime: Runtime with prediction API access

    Returns:
        Properties enriched with predicted sale prices
    """

    enriched_properties = []
    seq_no = 1

    for doc_idx, doc in enumerate(request.input):
        text = doc.text
        metadata = doc.metadata

        source_dmo_fields = {}
        if metadata and metadata.source_dmo_fields:
            source_dmo_fields = dict(metadata.source_dmo_fields)

        # Enrich with price prediction - pass source_dmo_fields directly
        citations = enrich_property_with_price(source_dmo_fields, runtime)

        # Create output
        property_output = SearchIndexChunkingV1Output(
            chunk_type=ChunkType.TEXT,
            text=text.strip(),
            seq_no=seq_no,
            citations=citations,
        )
        enriched_properties.append(property_output)

        seq_no += 1

    return SearchIndexChunkingV1Response(output=enriched_properties)
