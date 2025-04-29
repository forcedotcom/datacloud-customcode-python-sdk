from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame, SparkSession
import pytest

from datacustomcode.client import (
    Client,
    DataCloudAccessLayerException,
    DataCloudObjectType,
    _setup_spark,
)
from datacustomcode.config import (
    AccessLayerObjectConfig,
    ClientConfig,
    SparkConfig,
)
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode


class MockDataCloudReader(BaseDataCloudReader):
    """Mock reader for testing."""

    CONFIG_NAME = "MockDataCloudReader"

    def read_dlo(self, name: str) -> DataFrame:
        df = MagicMock(spec=DataFrame)
        return df

    def read_dmo(self, name: str) -> DataFrame:
        df = MagicMock(spec=DataFrame)
        return df


class MockDataCloudWriter(BaseDataCloudWriter):
    """Mock writer for testing."""

    CONFIG_NAME = "MockDataCloudWriter"

    def write_to_dlo(
        self, name: str, dataframe: DataFrame, write_mode: WriteMode, **kwargs
    ) -> None:
        pass

    def write_to_dmo(
        self, name: str, dataframe: DataFrame, write_mode: WriteMode, **kwargs
    ) -> None:
        pass


@pytest.fixture
def mock_spark():
    return MagicMock(spec=SparkSession)


@pytest.fixture
def mock_config(mock_spark):
    reader_config = AccessLayerObjectConfig(
        type_config_name="MockDataCloudReader", options={}, force=True
    )

    writer_config = AccessLayerObjectConfig(
        type_config_name="MockDataCloudWriter", options={}, force=True
    )

    spark_config = SparkConfig(
        app_name="test-app", master="local[1]", options={}, force=True
    )

    return ClientConfig(
        reader_config=reader_config,
        writer_config=writer_config,
        spark_config=spark_config,
    )


@pytest.fixture
def reset_client():
    """Reset the Client singleton between tests."""
    Client._instance = None
    yield
    Client._instance = None


class TestClient:

    def test_singleton_pattern(self, reset_client, mock_spark):
        """Test that Client behaves as a singleton."""
        reader = MockDataCloudReader(mock_spark)
        writer = MockDataCloudWriter(mock_spark)

        client1 = Client(reader=reader, writer=writer)
        client2 = Client()

        assert client1 is client2

        with pytest.raises(ValueError):
            Client(reader=MagicMock(spec=BaseDataCloudReader))

    @patch("datacustomcode.client.config")
    @patch("datacustomcode.client._setup_spark")
    def test_initialization_with_config(
        self, mock_setup_spark, mock_config, reset_client, mock_spark
    ):
        """Test client initialization using configuration."""
        mock_setup_spark.return_value = mock_spark

        mock_reader = MagicMock(spec=BaseDataCloudReader)
        mock_reader_config = MagicMock()
        mock_reader_config.to_object.return_value = mock_reader
        mock_reader_config.force = False

        mock_writer = MagicMock(spec=BaseDataCloudWriter)
        mock_writer_config = MagicMock()
        mock_writer_config.to_object.return_value = mock_writer
        mock_writer_config.force = False

        mock_spark_config = MagicMock(spec=SparkConfig)

        mock_config.reader_config = mock_reader_config
        mock_config.writer_config = mock_writer_config
        mock_config.spark_config = mock_spark_config

        client = Client()

        mock_setup_spark.assert_called_once_with(mock_spark_config)
        mock_reader_config.to_object.assert_called_once_with(mock_spark)
        mock_writer_config.to_object.assert_called_once_with(mock_spark)

        assert client._reader is mock_reader
        assert client._writer is mock_writer

    def test_read_dlo(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        reader.read_dlo.return_value = mock_df

        client = Client(reader=reader, writer=writer)
        result = client.read_dlo("test_dlo")

        reader.read_dlo.assert_called_once_with("test_dlo")
        assert result is mock_df
        assert "test_dlo" in client._data_layer_history[DataCloudObjectType.DLO]

    def test_read_dmo(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        reader.read_dmo.return_value = mock_df

        client = Client(reader=reader, writer=writer)
        result = client.read_dmo("test_dmo")

        reader.read_dmo.assert_called_once_with("test_dmo")
        assert result is mock_df
        assert "test_dmo" in client._data_layer_history[DataCloudObjectType.DMO]

    def test_write_to_dlo(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)

        client = Client(reader=reader, writer=writer)
        client._record_dlo_access("some_dlo")

        client.write_to_dlo("test_dlo", mock_df, WriteMode.APPEND, extra_param=True)

        writer.write_to_dlo.assert_called_once_with(
            "test_dlo", mock_df, WriteMode.APPEND, extra_param=True
        )

    def test_write_to_dmo(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)

        client = Client(reader=reader, writer=writer)
        client._record_dmo_access("some_dmo")

        client.write_to_dmo("test_dmo", mock_df, WriteMode.OVERWRITE, extra_param=True)

        writer.write_to_dmo.assert_called_once_with(
            "test_dmo", mock_df, WriteMode.OVERWRITE, extra_param=True
        )

    def test_mixed_dlo_dmo_raises_exception(self, reset_client, mock_spark):
        """Test that mixing DLOs and DMOs raises an exception."""
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)

        client = Client(reader=reader, writer=writer)
        client._record_dlo_access("test_dlo")

        with pytest.raises(DataCloudAccessLayerException) as exc_info:
            client.write_to_dmo("test_dmo", mock_df, WriteMode.APPEND)

        assert "test_dlo" in str(exc_info.value)

    def test_mixed_dmo_dlo_raises_exception(self, reset_client, mock_spark):
        """Test that mixing DMOs and DLOs raises an exception (converse case)."""
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)

        client = Client(reader=reader, writer=writer)
        client._record_dmo_access("test_dmo")

        with pytest.raises(DataCloudAccessLayerException) as exc_info:
            client.write_to_dlo("test_dlo", mock_df, WriteMode.APPEND)

        assert "test_dmo" in str(exc_info.value)

    def test_read_pattern_flow(self, reset_client, mock_spark):
        """Test a complete flow of reading and writing within the same object type."""
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        reader.read_dlo.return_value = mock_df

        client = Client(reader=reader, writer=writer)

        df = client.read_dlo("source_dlo")
        client.write_to_dlo("target_dlo", df, WriteMode.APPEND)

        reader.read_dlo.assert_called_once_with("source_dlo")
        writer.write_to_dlo.assert_called_once_with(
            "target_dlo", mock_df, WriteMode.APPEND
        )

        assert "source_dlo" in client._data_layer_history[DataCloudObjectType.DLO]

        # Reset for DMO test
        Client._instance = None
        client = Client(reader=reader, writer=writer)
        reader.read_dmo.return_value = mock_df

        df = client.read_dmo("source_dmo")
        client.write_to_dmo("target_dmo", df, WriteMode.MERGE)

        reader.read_dmo.assert_called_once_with("source_dmo")
        writer.write_to_dmo.assert_called_once_with(
            "target_dmo", mock_df, WriteMode.MERGE
        )

        assert "source_dmo" in client._data_layer_history[DataCloudObjectType.DMO]


# Add tests for _setup_spark function
class TestSetupSpark:

    @patch("datacustomcode.client.SparkSession")
    def test_setup_spark_with_master(self, mock_spark_session):
        """Test _setup_spark with master specified"""
        mock_builder = MagicMock()
        mock_master_builder = MagicMock()
        mock_app_name_builder = MagicMock()
        mock_config_builder = MagicMock()
        mock_session = MagicMock()

        mock_spark_session.builder = mock_builder
        mock_builder.master.return_value = mock_master_builder
        mock_master_builder.appName.return_value = mock_app_name_builder
        mock_app_name_builder.config.return_value = mock_config_builder
        mock_config_builder.getOrCreate.return_value = mock_session

        spark_config = SparkConfig(
            app_name="test-app",
            master="local[1]",
            options={"spark.executor.memory": "1g"},
        )

        result = _setup_spark(spark_config)

        mock_builder.master.assert_called_once_with("local[1]")
        mock_master_builder.appName.assert_called_once_with("test-app")
        mock_app_name_builder.config.assert_called_once_with(
            "spark.executor.memory", "1g"
        )
        mock_config_builder.getOrCreate.assert_called_once()
        assert result is mock_session

    @patch("datacustomcode.client.SparkSession")
    def test_setup_spark_with_multiple_options(self, mock_spark_session):
        """Test _setup_spark with multiple config options"""
        mock_builder = MagicMock()
        mock_app_name_builder = MagicMock()
        mock_config_builder1 = MagicMock()
        mock_config_builder2 = MagicMock()
        mock_config_builder3 = MagicMock()
        mock_session = MagicMock()

        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_app_name_builder
        mock_app_name_builder.config.return_value = mock_config_builder1
        mock_config_builder1.config.return_value = mock_config_builder2
        mock_config_builder2.config.return_value = mock_config_builder3
        mock_config_builder3.getOrCreate.return_value = mock_session

        spark_config = SparkConfig(
            app_name="test-app",
            master=None,
            options={
                "spark.executor.memory": "1g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "2g",
            },
        )

        result = _setup_spark(spark_config)

        mock_builder.appName.assert_called_once_with("test-app")

        # Check config was called for each option (order not guaranteed)
        assert mock_app_name_builder.config.call_count == 1
        assert mock_config_builder1.config.call_count == 1
        assert mock_config_builder2.config.call_count == 1

        mock_config_builder3.getOrCreate.assert_called_once()
        assert result is mock_session
