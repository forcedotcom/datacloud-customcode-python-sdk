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

import json
import logging
import subprocess
from typing import TYPE_CHECKING, Final, Optional, Union

import pandas as pd
import pandas.api.types as pd_types
import requests
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from datacustomcode.io.reader.base import BaseDataCloudReader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType

logger = logging.getLogger(__name__)


API_VERSION: Final = "v66.0"
PANDAS_TYPE_MAPPING = {
    "object": StringType(),
    "int64": LongType(),
    "float64": DoubleType(),
    "bool": BooleanType(),
}


def _pandas_to_spark_schema(
    pandas_df: pd.DataFrame, nullable: bool = True
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


class SFCLIDataCloudReader(BaseDataCloudReader):
    """DataCloud reader using SF CLI for authentication.

    This reader uses the Salesforce CLI (sf) to fetch fresh access tokens
    dynamically and queries Data Cloud using the REST API directly.

    Unlike the standard QueryAPIDataCloudReader which uses salesforcecdpconnector,
    this reader uses SF CLI tokens directly with Data Cloud REST endpoints,
    bypassing the CDP token exchange that requires special scopes.
    """

    CONFIG_NAME = "SFCLIDataCloudReader"

    def __init__(
        self,
        spark: SparkSession,
        org_alias: str,
        dataspace: Optional[str] = None,
    ) -> None:
        """Initialize SFCLIDataCloudReader.

        Args:
            spark: SparkSession instance for creating DataFrames.
            org_alias: Salesforce org alias as configured in SF CLI (e.g., from 'sf org list').
            dataspace: Optional dataspace identifier. If provided and not "default",
                queries will be executed within that dataspace.
                When None or "default", uses the default dataspace.
        """
        self.spark = spark
        self.org_alias = org_alias
        self.dataspace = dataspace if dataspace and dataspace != "default" else "default"
        logger.debug(f"Initialized SFCLIDataCloudReader for org alias '{org_alias}'")

    def _get_sf_cli_token(self) -> tuple[str, str]:
        """Fetch access token and instance URL from SF CLI.

        Returns:
            Tuple of (access_token, instance_url)

        Raises:
            RuntimeError: If SF CLI command fails or returns invalid data
        """
        try:
            result = subprocess.run(
                ["sf", "org", "display", "--target-org", self.org_alias, "--json"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
            org_data = json.loads(result.stdout)

            if org_data.get("status") != 0:
                raise RuntimeError(
                    f"SF CLI error: {org_data.get('message', 'Unknown error')}"
                )

            org_result = org_data.get("result", {})
            access_token = org_result.get("accessToken")
            instance_url = org_result.get("instanceUrl")

            if not access_token or not instance_url:
                raise RuntimeError(
                    "SF CLI did not return access token or instance URL"
                )

            logger.debug(
                f"Fetched fresh token from SF CLI for org '{self.org_alias}'"
            )
            return access_token, instance_url

        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"SF CLI command timed out for org '{self.org_alias}'"
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"SF CLI command failed: {e.stderr or e.stdout}"
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Failed to parse SF CLI output: {e}"
            )
        except FileNotFoundError:
            raise RuntimeError(
                "SF CLI ('sf' command) not found. Please install Salesforce CLI."
            )

    def _execute_query(self, sql: str, row_limit: int = 1000) -> pd.DataFrame:
        """Execute a SQL query against Data Cloud using REST API.

        Args:
            sql: SQL query to execute
            row_limit: Maximum number of rows to fetch

        Returns:
            Pandas DataFrame with query results

        Raises:
            RuntimeError: If query execution fails
        """
        access_token, instance_url = self._get_sf_cli_token()

        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{instance_url}/services/data/{API_VERSION}/ssot/query-sql"
        params = {"dataspace": self.dataspace}

        limited_sql = f"{sql} LIMIT {row_limit}"
        body = {"sql": limited_sql}

        logger.debug(f"Executing query: {limited_sql}")

        try:
            response = requests.post(
                url,
                json=body,
                params=params,
                headers=headers,
                timeout=120
            )

            if response.status_code >= 300:
                error_msg = response.text
                try:
                    error_data = response.json()
                    if isinstance(error_data, list) and len(error_data) > 0:
                        error_msg = error_data[0].get("message", error_msg)
                except (json.JSONDecodeError, KeyError):
                    pass

                raise RuntimeError(
                    f"Data Cloud query failed ({response.status_code}): {error_msg}"
                )

            result = response.json()
            rows = result.get("data", [])
            metadata = result.get("metadata", [])

            column_names = [col.get("name") for col in metadata]

            if not rows:
                return pd.DataFrame(columns=column_names)

            return pd.DataFrame(rows, columns=column_names)

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to execute Data Cloud query: {e}")

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
        sql = f"SELECT * FROM {name}"
        pandas_df = self._execute_query(sql, row_limit)

        if not schema:
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
        sql = f"SELECT * FROM {name}"
        pandas_df = self._execute_query(sql, row_limit)

        if not schema:
            schema = _pandas_to_spark_schema(pandas_df)
        spark_dataframe = self.spark.createDataFrame(pandas_df, schema)
        return spark_dataframe
