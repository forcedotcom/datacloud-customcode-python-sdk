from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datacustomcode.client import Client
from datacustomcode.config import (
    AccessLayerObjectConfig,
    ClientConfig,
    SparkConfig,
    SparkProviderConfig,
)
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode
from datacustomcode.spark.base import BaseSparkSessionProvider

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame


class _Sentinel:
    pass


SENTINEL_SPARK = _Sentinel()


class MockReader(BaseDataCloudReader):
    CONFIG_NAME = "MockReader"
    last_spark: Any | None = None

    def __init__(self, spark):
        super().__init__(spark)
        MockReader.last_spark = spark

    def read_dlo(self, name: str):  # type: ignore[override]
        raise NotImplementedError

    def read_dmo(self, name: str):  # type: ignore[override]
        raise NotImplementedError


class MockWriter(BaseDataCloudWriter):
    CONFIG_NAME = "MockWriter"
    last_spark: Any | None = None

    def __init__(self, spark):
        super().__init__(spark)
        MockWriter.last_spark = spark

    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:  # type: ignore[override]
        raise NotImplementedError

    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:  # type: ignore[override]
        raise NotImplementedError


class FakeProvider(BaseSparkSessionProvider):
    CONFIG_NAME = "FakeProvider"

    def get_session(self, spark_config: SparkConfig):  # type: ignore[override]
        return SENTINEL_SPARK


def _reset_singleton():
    # Reset Client singleton between tests
    Client._instance = None  # type: ignore[attr-defined]


def test_client_uses_provider_from_config(monkeypatch):
    _reset_singleton()

    cfg = ClientConfig(
        reader_config=AccessLayerObjectConfig(
            type_config_name=MockReader.CONFIG_NAME, options={}
        ),
        writer_config=AccessLayerObjectConfig(
            type_config_name=MockWriter.CONFIG_NAME, options={}
        ),
        spark_config=SparkConfig(app_name="test-app", master=None, options={}),
        spark_provider_config=SparkProviderConfig(
            type_config_name=FakeProvider.CONFIG_NAME, options={}
        ),
    )

    from datacustomcode.config import config as global_config

    global_config.update(cfg)

    Client()
    assert MockReader.last_spark is SENTINEL_SPARK
    assert MockWriter.last_spark is SENTINEL_SPARK


class ExplicitProvider(BaseSparkSessionProvider):
    CONFIG_NAME = "ExplicitProvider"

    def get_session(self, spark_config: SparkConfig):  # type: ignore[override]
        return SENTINEL_SPARK


def test_client_explicit_provider_overrides_config(monkeypatch):
    _reset_singleton()

    cfg = ClientConfig(
        reader_config=AccessLayerObjectConfig(
            type_config_name=MockReader.CONFIG_NAME, options={}
        ),
        writer_config=AccessLayerObjectConfig(
            type_config_name=MockWriter.CONFIG_NAME, options={}
        ),
        spark_config=SparkConfig(app_name="test-app", master=None, options={}),
        spark_provider_config=None,
    )

    from datacustomcode.config import config as global_config

    global_config.update(cfg)

    provider = ExplicitProvider()
    Client(spark_provider=provider)
    assert MockReader.last_spark is SENTINEL_SPARK
    assert MockWriter.last_spark is SENTINEL_SPARK
