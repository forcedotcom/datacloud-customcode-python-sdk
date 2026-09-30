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

import tempfile
from typing import TYPE_CHECKING, Optional

from datacustomcode.io import cdf
from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_HEADER_WARNING = (
    "  NOTE: Once this code extension is deployed, Data Cloud will persist "
    "only the final state per primary key.\n"
    "  This local preview shows every emitted row uncollapsed."
)


class LocalDeltasWriter(BaseDataCloudWriter):
    """Local writer for streaming transforms."""

    CONFIG_NAME = "LocalDeltasWriter"

    def __init__(
        self,
        spark: SparkSession,
        credentials_profile: str = "default",
        dataspace: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
    ) -> None:
        super().__init__(spark)

        from datacustomcode.io.writer.print import PrintDataCloudWriter

        self._batch_writer = PrintDataCloudWriter(
            spark=spark,
            credentials_profile=credentials_profile,
            dataspace=dataspace,
            sf_cli_org=sf_cli_org,
        )

    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        return self._batch_writer.write_to_dlo(name, dataframe, write_mode)

    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        return self._batch_writer.write_to_dmo(name, dataframe, write_mode)

    def auto_write_to_dlo(self, name: str, dataframe: PySparkDataFrame) -> None:
        return self._batch_writer.auto_write_to_dlo(name, dataframe)

    def auto_write_to_dmo(self, name: str, dataframe: PySparkDataFrame) -> None:
        return self._batch_writer.auto_write_to_dmo(name, dataframe)

    def write_dlo_deltas(
        self, name: str, dataframe: PySparkDataFrame, **kwargs
    ) -> StreamingQuery:
        if not name:
            raise ValueError("DLO name must be provided.")

        def _preview(batch_df, batch_id):
            from pyspark.sql import functions as F

            output_batch = batch_df.withColumn(
                "_operation",
                F.when(
                    F.col(cdf.MERGE_RECORD_TYPE) == cdf.MergeRecordType.DELETE.value,
                    F.lit("DELETE"),
                ).otherwise(F.lit("UPSERT")),
            ).drop(
                cdf.COMMIT_VERSION,
                cdf.COMMIT_TIMESTAMP,
                cdf.MERGE_RECORD_TYPE,
            )

            row_count = output_batch.count()
            print(f"\nTarget={name} batch_id={batch_id} rows={row_count}")
            print(_HEADER_WARNING)
            output_batch.show(truncate=False)

        checkpoint_dir = tempfile.mkdtemp(prefix=f"local-deltas-ckpt-{name}-")
        return (
            dataframe.writeStream.foreachBatch(_preview)
            .option("checkpointLocation", checkpoint_dir)
            # AvailableNow: drain every fixture file then
            # terminate. The user's `query.awaitTermination()`
            # returns without needing a timeout, so
            # `datacustomcode run` exits deterministically.
            .trigger(availableNow=True)
            .start()
        )
