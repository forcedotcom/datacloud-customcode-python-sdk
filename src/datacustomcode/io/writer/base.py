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

from abc import abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from datacustomcode.io.base import BaseDataAccessLayer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_PARTITIONS = "overwrite_partitions"  # Deprecated: raises error if used
    MERGE = "merge"
    MERGE_UPSERT_DELETE = "merge_upsert_delete"


class MergeRecordType(str, Enum):
    """Values for the _merge_record_type column used by MERGE_UPSERT_DELETE."""

    UPSERT = "UPSERT"
    DELETE = "DELETE"


MERGE_RECORD_TYPE_COLUMN = "_merge_record_type"


class BaseDataCloudWriter(BaseDataAccessLayer):
    """Base class for Data Cloud writers."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None: ...

    @abstractmethod
    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None: ...

    def write_dlo_deltas(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> StreamingQuery:
        """Write a streaming DataFrame of deltas to a Data Lake Object.

        Streaming counterpart to :meth:`write_to_dlo`. Starts a streaming query
        that writes each micro-batch to the target DLO via the Data Cloud
        streaming sink and returns the resulting ``StreamingQuery`` handle. The
        runtime owns the trigger and checkpoint location; callers pass only the
        table name and write mode. Concrete streaming behavior is provided by
        the deployed Data Cloud runtime; the base implementation raises
        :class:`NotImplementedError`.

        Args:
            name: Target Data Lake Object name.
            dataframe: Streaming PySpark DataFrame produced from a
                ``read_dlo_deltas`` / ``read_dmo_deltas`` source.
            write_mode: Write mode for the streaming sink. Supported modes are
                ``WriteMode.APPEND``, ``WriteMode.OVERWRITE``, and
                ``WriteMode.MERGE_UPSERT_DELETE``.

        Returns:
            The started ``StreamingQuery``; the caller drives its lifecycle
            (typically ``query.awaitTermination()``).

        Raises:
            NotImplementedError: If the active writer does not support streaming
                deltas (e.g. the local development writers).
        """
        raise NotImplementedError(
            "write_dlo_deltas is only supported when running in the Data Cloud "
            "streaming runtime; the local writer does not support streaming "
            "deltas."
        )
