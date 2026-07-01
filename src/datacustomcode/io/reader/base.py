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
from typing import TYPE_CHECKING, Union

from datacustomcode.io.base import BaseDataAccessLayer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType, StructType


class BaseDataCloudReader(BaseDataAccessLayer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame: ...

    @abstractmethod
    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame: ...

    def read_dlo_deltas(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        """Read the streaming change feed (deltas) for a Data Lake Object.

        This is the streaming counterpart to :meth:`read_dlo`. It returns a
        streaming DataFrame over the change feed the Data Cloud runtime
        publishes for a streaming (``DELTA_SYNC``) transform. Concrete
        streaming behavior is provided by the deployed Data Cloud runtime; the
        base implementation raises :class:`NotImplementedError` so local
        readers that do not support streaming fail clearly.

        Args:
            name: Data Lake Object name.
            schema: Accepted for parity with :meth:`read_dlo`; implementations
                may ignore it.

        Returns:
            A streaming PySpark DataFrame over the DLO change feed.

        Raises:
            NotImplementedError: If the active reader does not support streaming
                deltas (e.g. the local development readers).
        """
        raise NotImplementedError(
            "read_dlo_deltas is only supported when running in the Data Cloud "
            "streaming runtime; the local reader does not support streaming "
            "deltas."
        )

    def read_dmo_deltas(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        """Read the streaming change feed (deltas) for a Data Model Object.

        Streaming counterpart to :meth:`read_dmo`. See :meth:`read_dlo_deltas`
        for behavior and the local-development caveat.

        Args:
            name: Data Model Object name.
            schema: Accepted for parity with :meth:`read_dmo`; implementations
                may ignore it.

        Returns:
            A streaming PySpark DataFrame over the DMO change feed.

        Raises:
            NotImplementedError: If the active reader does not support streaming
                deltas (e.g. the local development readers).
        """
        raise NotImplementedError(
            "read_dmo_deltas is only supported when running in the Data Cloud "
            "streaming runtime; the local reader does not support streaming "
            "deltas."
        )
