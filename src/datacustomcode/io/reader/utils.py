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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas
    from pyspark.sql.types import AtomicType, StructType


def _pandas_to_spark_schema(
    pandas_df: pandas.DataFrame, nullable: bool = True
) -> StructType:
    import pandas.api.types as pd_types
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    PANDAS_TYPE_MAPPING = {
        "object": StringType(),
        "int64": LongType(),
        "float64": DoubleType(),
        "bool": BooleanType(),
    }

    fields = []
    for column, dtype in pandas_df.dtypes.items():
        spark_type: AtomicType
        if pd_types.is_datetime64_any_dtype(dtype):
            spark_type = TimestampType()
        else:
            spark_type = PANDAS_TYPE_MAPPING.get(str(dtype), StringType())
        fields.append(StructField(column.lower(), spark_type, nullable))
    return StructType(fields)
