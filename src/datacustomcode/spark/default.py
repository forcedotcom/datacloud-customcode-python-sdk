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


