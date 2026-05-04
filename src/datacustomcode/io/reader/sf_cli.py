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
from typing import (
    TYPE_CHECKING,
    Final,
    Optional,
    Union,
)

import requests

from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.reader.utils import _pandas_to_spark_schema
from datacustomcode.token_provider import SFCLITokenProvider

if TYPE_CHECKING:
    import pandas as pd
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
        default_row_limit: Optional[int] = None,
    ) -> None:
        """Initialize SFCLIDataCloudReader.

        Args:
            spark: SparkSession instance for creating DataFrames.
            sf_cli_org: Salesforce org alias or username as known to the SF CLI
                (e.g. the alias given to ``sf org login web --alias dev1``).
            dataspace: Optional dataspace identifier.  If ``None`` or
                ``"default"`` the query runs against the default dataspace.
            default_row_limit: Maximum number of rows to fetch automatically.
                When ``None``, no limit is applied (all rows are returned).
                Set via ``default_row_limit`` in ``config.yaml`` reader options.
        """
        self.spark = spark
        self.sf_cli_org = sf_cli_org
        self._default_row_limit = default_row_limit
        self.dataspace = (
            dataspace if dataspace and dataspace != "default" else "default"
        )
        logger.debug(f"Initialized SFCLIDataCloudReader for org '{sf_cli_org}'")

    def _get_token(self) -> tuple[str, str]:
        token_response = SFCLITokenProvider(self.sf_cli_org).get_token()
        logger.debug(f"Fetched token from SF CLI for org '{self.sf_cli_org}'")
        return token_response.access_token, token_response.instance_url

    def _execute_query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the Data Cloud REST endpoint.

        The configured ``default_row_limit`` is automatically appended as a
        ``LIMIT`` clause when set (typically for local development).

        Args:
            sql: Base SQL query (no ``LIMIT`` clause).

        Returns:
            Pandas DataFrame with query results.

        Raises:
            RuntimeError: On HTTP errors or unexpected response shapes.
        """
        import pandas as pd

        access_token, instance_url = self._get_token()

        url = f"{instance_url}/services/data/{API_VERSION}/ssot/query-sql"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"dataspace": self.dataspace}
        if self._default_row_limit is not None:
            body = {"sql": f"{sql} LIMIT {self._default_row_limit}"}
        else:
            body = {"sql": sql}

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
    ) -> PySparkDataFrame:
        """Read a Data Lake Object (DLO) from Data Cloud.

        Args:
            name: DLO name.
            schema: Optional explicit schema.

        Returns:
            PySpark DataFrame.
        """
        pandas_df = self._execute_query(f"SELECT * FROM {name}")
        if not schema:
            schema = _pandas_to_spark_schema(pandas_df)
        return self.spark.createDataFrame(pandas_df, schema)

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        """Read a Data Model Object (DMO) from Data Cloud.

        Args:
            name: DMO name.
            schema: Optional explicit schema.

        Returns:
            PySpark DataFrame.
        """
        pandas_df = self._execute_query(f"SELECT * FROM {name}")
        if not schema:
            schema = _pandas_to_spark_schema(pandas_df)
        return self.spark.createDataFrame(pandas_df, schema)
