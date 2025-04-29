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
from typing import TYPE_CHECKING

from datacustomcode.io.base import BaseDataAccessLayer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession


class BaseDataCloudReader(BaseDataAccessLayer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read_dlo(self, name: str) -> PySparkDataFrame: ...

    @abstractmethod
    def read_dmo(self, name: str) -> PySparkDataFrame: ...
