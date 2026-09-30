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
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from datacustomcode.config import config
from datacustomcode.io import cdf
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.reader.query_api import QueryAPIDataCloudReader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType, StructType

logger = logging.getLogger(__name__)


class LocalDeltasReader(BaseDataCloudReader):
    """Local reader for streaming transforms"""

    CONFIG_NAME = "LocalDeltasReader"

    def __init__(
        self,
        spark: SparkSession,
        credentials_profile: str = "default",
        dataspace: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
        default_row_limit: Optional[int] = None,
        fixtures_root: str = "payload/streaming_fixtures",
    ) -> None:
        super().__init__(spark)
        self._fixtures_root = Path(fixtures_root)
        self._credentials_profile = credentials_profile
        self._dataspace = dataspace
        self._sf_cli_org = sf_cli_org
        # Reuse the batch reader for INITIAL_SYNC / REBUILD reads.
        self._batch = QueryAPIDataCloudReader(
            spark=spark,
            credentials_profile=credentials_profile,
            dataspace=dataspace,
            sf_cli_org=sf_cli_org,
            default_row_limit=default_row_limit,
        )
        self._current_layer = "dlo"

    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        return self._batch.read_dlo(name, schema)

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        return self._batch.read_dmo(name, schema)

    def read_dlo_deltas(self) -> PySparkDataFrame:
        self._current_layer = "dlo"
        return self._open_stream(self._streaming_source())

    def read_dmo_deltas(self) -> PySparkDataFrame:
        self._current_layer = "dmo"
        return self._open_stream(self._streaming_source())

    def _streaming_source(self) -> str:
        source = config.streaming_source
        if not source:
            raise RuntimeError(
                "No streaming source configured. Set streamingSource.name in "
                "config.json."
            )
        return source

    def _open_stream(self, name: str) -> PySparkDataFrame:
        drop_dir = self._fixtures_root / name

        if not (drop_dir / "_schema.json").exists():
            seeded = self._try_seed(name)
            if not seeded:
                print(
                f"\nStreaming source {self._current_layer}='{name}' is empty.\n"
                f"  Populate the stream source with at least one row in Data Cloud, "
                f"then re-run `datacustomcode run`.\n"
                )
                sys.exit(0)

            print(
                f"\nWrote sample streaming fixture in: {drop_dir} "
                f"based on the contents of the streaming source {name}.\n"
                f"  You may add more JSON files alongside it to simulate additional "
                f"changes.\n"
            )

        schema = self._build_stream_schema(name)
        return (
            self.spark.readStream
                .format("json")
                .schema(schema)
                .option("maxFilesPerTrigger", 1) # one file = one batch
                .option("latestFirst", "false")  # oldest mtime first
                .load(str(drop_dir))
        )

    def _build_stream_schema(self, name: str) -> "StructType":
        """Compose (source schema + CDF metadata columns) for readStream."""
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        source_schema = self._resolve_source_schema(name)
        cdf_fields = [
            StructField(cdf.COMMIT_VERSION, LongType(), True),
            StructField(cdf.COMMIT_TIMESTAMP, TimestampType(), True),
            StructField(cdf.MERGE_RECORD_TYPE, StringType(), True),
        ]
        return StructType(list(source_schema.fields) + cdf_fields)

    def _resolve_source_schema(self, name: str) -> "StructType":
        from pyspark.sql.types import StructType

        schema_file = self._fixtures_root / name / "_schema.json"
        return StructType.fromJson(json.loads(schema_file.read_text()))

    def _try_seed(self, name: str) -> bool:
        """Creates source schema and sample change file."""
        from datacustomcode.io.reader.streaming_seeder import (
            StreamingSourceSeeder,
        )

        seeder = StreamingSourceSeeder(
            spark=self.spark,
            credentials_profile=self._credentials_profile,
            dataspace=self._dataspace,
            sf_cli_org=self._sf_cli_org,
        )
        return seeder.seed_source(
            name, self._current_layer, str(self._fixtures_root)
        )
