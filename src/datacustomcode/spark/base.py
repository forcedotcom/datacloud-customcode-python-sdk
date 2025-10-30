from __future__ import annotations

from typing import TYPE_CHECKING

from datacustomcode.mixin import UserExtendableNamedConfigMixin

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from datacustomcode.config import SparkConfig


class BaseSparkSessionProvider(UserExtendableNamedConfigMixin):
    def get_session(self, spark_config: SparkConfig) -> "SparkSession":
        raise NotImplementedError


