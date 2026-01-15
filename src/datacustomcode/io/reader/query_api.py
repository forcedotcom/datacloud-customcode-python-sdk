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
    Optional,
    Union,
)

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
from salesforcecdpconnector.connection import SalesforceCDPConnection

from datacustomcode.credentials import AuthType, Credentials
from datacustomcode.io.reader.base import BaseDataCloudReader

if TYPE_CHECKING:
    import pandas
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType

logger = logging.getLogger(__name__)


SQL_QUERY_TEMPLATE: Final = "SELECT * FROM {} LIMIT {}"
PANDAS_TYPE_MAPPING = {
    "object": StringType(),
    "int64": LongType(),
    "float64": DoubleType(),
    "bool": BooleanType(),
}


def _pandas_to_spark_schema(
    pandas_df: pandas.DataFrame, nullable: bool = True
) -> StructType:
    fields = []
    for column, dtype in pandas_df.dtypes.items():
        spark_type: AtomicType
        if pd_types.is_datetime64_any_dtype(dtype):
            spark_type = TimestampType()
        else:
            spark_type = PANDAS_TYPE_MAPPING.get(str(dtype), StringType())
        fields.append(StructField(column, spark_type, nullable))
    return StructType(fields)


def create_cdp_connection(
    credentials: Credentials,
    dataspace: Optional[str] = None,
) -> SalesforceCDPConnection:
    """Create a SalesforceCDPConnection based on the credentials auth type.

    This factory function creates the appropriate connection based on the
    authentication method configured in the credentials.

    Args:
        credentials: Credentials instance with authentication details.
        dataspace: Optional dataspace identifier for multi-tenant queries.
            If None or "default", the dataspace argument is not passed to
            the connection constructor.

    Returns:
        SalesforceCDPConnection configured for the specified auth method.

    Raises:
        ValueError: If the auth type is not supported.
    """
    effective_dataspace = dataspace if dataspace and dataspace != "default" else None

    if credentials.auth_type == AuthType.OAUTH_TOKENS:
        logger.debug("Creating CDP connection with OAuth Tokens authentication")
        if effective_dataspace is not None:
            return SalesforceCDPConnection(
                credentials.login_url,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
                refresh_token=credentials.refresh_token,
                dataspace=effective_dataspace,
            )
        else:
            return SalesforceCDPConnection(
                credentials.login_url,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
                refresh_token=credentials.refresh_token,
            )

    elif credentials.auth_type == AuthType.CLIENT_CREDENTIALS:
        logger.debug("Creating CDP connection with Client Credentials authentication")
        if effective_dataspace is not None:
            return SalesforceCDPConnection(
                credentials.login_url,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
                dataspace=effective_dataspace,
            )
        else:
            return SalesforceCDPConnection(
                credentials.login_url,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
            )

    else:
        raise ValueError(f"Unsupported authentication type: {credentials.auth_type}")


class QueryAPIDataCloudReader(BaseDataCloudReader):
    """DataCloud reader using Query API.

    This reader emulates data access within Data Cloud by calling the Query API.
    Supports multiple authentication methods:
    - OAuth Tokens (default, needs client_id/secret with refresh_token)
    - Client Credentials (server-to-server, needs client_id/secret only)

    Supports dataspace configuration for querying data within specific dataspaces.
    When a dataspace is provided (and not "default"), queries are executed within
    that dataspace context.
    """

    CONFIG_NAME = "QueryAPIDataCloudReader"

    def __init__(
        self,
        spark: SparkSession,
        credentials_profile: str = "default",
        dataspace: Optional[str] = None,
    ) -> None:
        """Initialize QueryAPIDataCloudReader.

        Args:
            spark: SparkSession instance for creating DataFrames.
            credentials_profile: Credentials profile name (default: "default").
                The profile determines which credentials to load from the
                ~/.datacustomcode/credentials.ini file or environment variables.
            dataspace: Optional dataspace identifier. If provided and not "default",
                the connection will be configured for the specified dataspace.
                When None or "default", uses the default dataspace.
        """
        self.spark = spark
        credentials = Credentials.from_available(profile=credentials_profile)
        logger.debug(
            "Initializing QueryAPIDataCloudReader with "
            f"auth_type={credentials.auth_type.value}"
        )
        self._conn = create_cdp_connection(credentials, dataspace)

    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
        row_limit: int = 1000,
    ) -> PySparkDataFrame:
        """
        Read a Data Lake Object (DLO) from the Data Cloud, limited to a number of rows.

        Args:
            name (str): The name of the DLO.
            schema (Optional[Union[AtomicType, StructType, str]]): Schema of the DLO.
            row_limit (int): Maximum number of rows to fetch.

        Returns:
            PySparkDataFrame: The PySpark DataFrame.
        """
        query = SQL_QUERY_TEMPLATE.format(name, row_limit)

        pandas_df = self._conn.get_pandas_dataframe(query)

        # Convert pandas DataFrame to Spark DataFrame
        if not schema:
            # auto infer schema
            schema = _pandas_to_spark_schema(pandas_df)
        spark_dataframe = self.spark.createDataFrame(pandas_df, schema)
        return spark_dataframe

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
        row_limit: int = 1000,
    ) -> PySparkDataFrame:
        """
        Read a Data Model Object (DMO) from the Data Cloud, limited to a number of rows.

        Args:
            name (str): The name of the DMO.
            schema (Optional[Union[AtomicType, StructType, str]]): Schema of the DMO.
            row_limit (int): Maximum number of rows to fetch.

        Returns:
            PySparkDataFrame: The PySpark DataFrame.
        """
        query = SQL_QUERY_TEMPLATE.format(name, row_limit)

        pandas_df = self._conn.get_pandas_dataframe(query)

        # Convert pandas DataFrame to Spark DataFrame
        if not schema:
            # auto infer schema
            schema = _pandas_to_spark_schema(pandas_df)
        spark_dataframe = self.spark.createDataFrame(pandas_df, schema)
        return spark_dataframe
