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

"""LLM completion functions for Data Cloud Custom Code.
"""

from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import call_function, lit

# Default values for llm_complete function
# TODO: Validate these defaults
_DEFAULT_MODEL_ID = "sfdc_ai__DefaultGPT4Omni"
_DEFAULT_MAX_TOKENS = 200
_LLM_GATEWAY_UDF_NAME = "llm_gateway_generate"


def llm_complete(
    prompt_col: Union[Column, str],
    *,
    model_id: str = _DEFAULT_MODEL_ID,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
) -> Column:
    """Returns the AI-generated text response as a string column.

    Args:
        prompt_col: Column or column name containing the prompt text.
            The prompt should be a string value (max 32KB recommended).
            Use string functions like concat_ws(), format_string(), etc.
            to construct complex prompts from multiple columns.
        model_id: Defaults to "sfdc_ai__DefaultGPT4Omni".
            Available models depend on your org's configuration.
        max_tokens: Maximum tokens in the response. Defaults to 200.
            Higher values allow longer responses but increase latency and cost.

    Returns:
        Column of StringType with AI-generated response.
        Returns null if the input prompt is null.

    Raises:
        TypeError: If prompt_col is not a Column or string.
        ValueError: If max_tokens is not positive.
    """
    # Input validation
    if not isinstance(prompt_col, (Column, str)):
        raise TypeError(
            f"prompt_col must be a Column or str, got {type(prompt_col).__name__}"
        )

    if not isinstance(max_tokens, int) or max_tokens <= 0:
        raise ValueError(f"max_tokens must be a positive integer, got {max_tokens}")

    # Convert string column name to Column
    if isinstance(prompt_col, str):
        from pyspark.sql.functions import col

        prompt_col = col(prompt_col)

    from pyspark.sql.functions import named_struct

    template = "{prompt}"
    values_struct = named_struct(lit("prompt"), prompt_col)

    return call_function(
        _LLM_GATEWAY_UDF_NAME,
        lit(template),
        values_struct,
        lit(model_id),
        lit(max_tokens),
    )
