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

import logging
from typing import (
    TYPE_CHECKING,
    Final,
    Union,
)

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from salesforcecdpconnector.connection import SalesforceCDPConnection

from datacustomcode.credentials import Credentials
from datacustomcode.io.reader.base import BaseDataCloudReader

if TYPE_CHECKING:
    import pandas
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType

logger = logging.getLogger(__name__)


SQL_QUERY_TEMPLATE: Final = "SELECT * FROM {}"
PANDAS_TYPE_MAPPING = {
    "object": StringType(),
    "int64": LongType(),
    "float64": DoubleType(),
    "datetime64[ns]": TimestampType(),
    "datetime64[ns, UTC]": TimestampType(),
    "bool": BooleanType(),
}


def _pandas_to_spark_schema(
    pandas_df: pandas.DataFrame, nullable: bool = True
) -> StructType:
    fields = []
    for column, dtype in pandas_df.dtypes.items():
        spark_type = PANDAS_TYPE_MAPPING.get(str(dtype), StringType())
        fields.append(StructField(column, spark_type, nullable))
    return StructType(fields)


class QueryAPIDataCloudReader(BaseDataCloudReader):
    """DataCloud reader using Query API.

    This reader emulates data access within Data Cloud by calling the Query API.
    """

    CONFIG_NAME = "QueryAPIDataCloudReader"

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        credentials = Credentials.from_available()

        self._conn = SalesforceCDPConnection(
            credentials.login_url,
            credentials.username,
            credentials.password,
            credentials.client_id,
            credentials.client_secret,
        )

    def read_dlo(
        self, name: str, schema: Union[AtomicType, StructType, str, None] = None
    ) -> PySparkDataFrame:
        """
        Read a Data Lake Object (DLO) from the Data Cloud.

        Args:
            name (str): The name of the DLO.
            schema (Optional[Union[AtomicType, StructType, str]]): Schema of the DLO.

        Returns:
            PySparkDataFrame: The PySpark DataFrame.
        """
        pandas_df = self._conn.get_pandas_dataframe(SQL_QUERY_TEMPLATE.format(name))
        if not schema:
            # auto infer schema
            schema = _pandas_to_spark_schema(pandas_df)
        spark_dataframe = self.spark.createDataFrame(pandas_df, schema)
        return spark_dataframe

    def read_dmo(
        self, name: str, schema: Union[AtomicType, StructType, str, None] = None
    ) -> PySparkDataFrame:
        pandas_df = self._conn.get_pandas_dataframe(SQL_QUERY_TEMPLATE.format(name))
        if not schema:
            # auto infer schema
            schema = _pandas_to_spark_schema(pandas_df)
        spark_dataframe = self.spark.createDataFrame(pandas_df, schema)
        return spark_dataframe
