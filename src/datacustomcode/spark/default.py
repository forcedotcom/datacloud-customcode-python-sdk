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

from typing import TYPE_CHECKING

from datacustomcode.spark.base import BaseSparkSessionProvider

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from datacustomcode.config import SparkConfig


class DefaultSparkSessionProvider(BaseSparkSessionProvider):
    CONFIG_NAME = "DefaultSparkSessionProvider"

    def get_session(self, spark_config: SparkConfig) -> "SparkSession":
        from pyspark.sql import SparkSession

        builder = SparkSession.builder
        if spark_config.master is not None:
            builder = builder.master(spark_config.master)
        builder = builder.appName(spark_config.app_name)
        for key, value in spark_config.options.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
