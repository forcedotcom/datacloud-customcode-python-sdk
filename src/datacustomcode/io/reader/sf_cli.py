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
from typing import (
    TYPE_CHECKING,
    Final,
    Optional,
    Union,
)

import pandas as pd
import requests

from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.reader.utils import _pandas_to_spark_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType, StructType

logger = logging.getLogger(__name__)

API_VERSION: Final = "v66.0"


class SFCLIDataCloudReader(BaseDataCloudReader):
    """DataCloud reader that authenticates via the Salesforce CLI.

    Uses ``sf org display`` to obtain a fresh access token and queries
    Data Cloud through the REST API directly
    (``/services/data/{version}/ssot/query-sql``), bypassing the CDP
    token-exchange flow that requires special OAuth scopes.
    """

    CONFIG_NAME = "SFCLIDataCloudReader"

    def __init__(
        self,
        spark: SparkSession,
        sf_cli_org: str,
        dataspace: Optional[str] = None,
    ) -> None:
        """Initialize SFCLIDataCloudReader.

        Args:
            spark: SparkSession instance for creating DataFrames.
            sf_cli_org: Salesforce org alias or username as known to the SF CLI
                (e.g. the alias given to ``sf org login web --alias dev1``).
            dataspace: Optional dataspace identifier.  If ``None`` or
                ``"default"`` the query runs against the default dataspace.
        """
        self.spark = spark
        self.sf_cli_org = sf_cli_org
        self.dataspace = (
            dataspace if dataspace and dataspace != "default" else "default"
        )
        logger.debug(f"Initialized SFCLIDataCloudReader for org '{sf_cli_org}'")

    def _get_token(self) -> tuple[str, str]:
        """Fetch a fresh access token and instance URL from the SF CLI.

        Returns:
            ``(access_token, instance_url)``

        Raises:
            RuntimeError: If the ``sf`` command is not on PATH, times out, or
                returns an error.
        """
        try:
            result = subprocess.run(
                ["sf", "org", "display", "--target-org", self.sf_cli_org, "--json"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "The 'sf' command was not found.  "
                "Please install Salesforce CLI: https://developer.salesforce.com/tools/salesforcecli"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"'sf org display' timed out for org '{self.sf_cli_org}'"
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"'sf org display' failed for org '{self.sf_cli_org}'.\n"
                f"Ensure the org is authenticated via 'sf org login web'.\n"
                f"stderr: {exc.stderr.strip()}"
            ) from exc

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse 'sf org display' output: {exc}"
            ) from exc

        if data.get("status") != 0:
            raise RuntimeError(
                f"SF CLI error for org '{self.sf_cli_org}': "
                f"{data.get('message', 'unknown error')}"
            )

        org_result = data.get("result", {})
        access_token = org_result.get("accessToken")
        instance_url = org_result.get("instanceUrl")

        if not access_token or not instance_url:
            raise RuntimeError(
                f"'sf org display' did not return an access token or instance URL "
                f"for org '{self.sf_cli_org}'"
            )

        logger.debug(f"Fetched token from SF CLI for org '{self.sf_cli_org}'")
        return access_token, instance_url

    def _execute_query(self, sql: str, row_limit: int) -> pd.DataFrame:
        """Execute *sql* against the Data Cloud REST endpoint.

        Args:
            sql: Base SQL query (no ``LIMIT`` clause).
            row_limit: Maximum rows to return.

        Returns:
            Pandas DataFrame with query results.

        Raises:
            RuntimeError: On HTTP errors or unexpected response shapes.
        """
        access_token, instance_url = self._get_token()

        url = f"{instance_url}/services/data/{API_VERSION}/ssot/query-sql"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"dataspace": self.dataspace}
        body = {"sql": f"{sql} LIMIT {row_limit}"}

        logger.debug(f"Executing Data Cloud query: {body['sql']}")

        try:
            response = requests.post(
                url,
                json=body,
                params=params,
                headers=headers,
                timeout=120,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Data Cloud query request failed: {exc}") from exc

        if response.status_code >= 300:
            error_msg = response.text
            try:
                error_data = response.json()
                if isinstance(error_data, list) and error_data:
                    error_msg = error_data[0].get("message", error_msg)
            except (json.JSONDecodeError, KeyError):
                pass
            raise RuntimeError(
                f"Data Cloud query failed (HTTP {response.status_code}): {error_msg}"
            )

        result = response.json()
        metadata = result.get("metadata", [])
        column_names = [col.get("name") for col in metadata]
        rows = result.get("data", [])

        if not rows:
            return pd.DataFrame(columns=column_names)
        return pd.DataFrame(rows, columns=column_names)

    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
        row_limit: int = 1000,
    ) -> PySparkDataFrame:
        """Read a Data Lake Object (DLO) from Data Cloud.

        Args:
            name: DLO name.
            schema: Optional explicit schema.
            row_limit: Maximum rows to fetch.

        Returns:
            PySpark DataFrame.
        """
        pandas_df = self._execute_query(f"SELECT * FROM {name}", row_limit)
        if not schema:
            schema = _pandas_to_spark_schema(pandas_df)
        return self.spark.createDataFrame(pandas_df, schema)

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
        row_limit: int = 1000,
    ) -> PySparkDataFrame:
        """Read a Data Model Object (DMO) from Data Cloud.

        Args:
            name: DMO name.
            schema: Optional explicit schema.
            row_limit: Maximum rows to fetch.

        Returns:
            PySpark DataFrame.
        """
        pandas_df = self._execute_query(f"SELECT * FROM {name}", row_limit)
        if not schema:
            schema = _pandas_to_spark_schema(pandas_df)
        return self.spark.createDataFrame(pandas_df, schema)
