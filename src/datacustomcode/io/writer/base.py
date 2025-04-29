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


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_PARTITIONS = "overwrite_partitions"
    MERGE = "merge"
    MERGE_UPSERT_DELETE = "merge_upsert_delete"


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
