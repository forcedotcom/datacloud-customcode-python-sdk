from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame, SparkSession
import pytest

from datacustomcode.client import (
    Client,
    DataCloudAccessLayerException,
    DataCloudObjectType,
    einstein_predict_col,
    llm_gateway_generate_text_col,
)
from datacustomcode.config import (
    AccessLayerObjectConfig,
    ClientConfig,
    SparkConfig,
)
from datacustomcode.einstein_predictions.types import PredictionType
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode


class MockDataCloudReader(BaseDataCloudReader):
    """Mock reader for testing."""

    CONFIG_NAME = "MockDataCloudReader"

    def read_dlo(self, name: str, schema=None) -> DataFrame:
        df = MagicMock(spec=DataFrame)
        return df

    def read_dmo(self, name: str, schema=None) -> DataFrame:
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
    def test_initialization_with_config(self, mock_config, reset_client, mock_spark):
        """Test client initialization using configuration."""
        from unittest.mock import patch as mock_patch

        from datacustomcode.spark.default import DefaultSparkSessionProvider

        with mock_patch.object(
            DefaultSparkSessionProvider, "get_session"
        ) as mock_get_session:
            mock_get_session.return_value = mock_spark

            mock_reader = MagicMock(spec=BaseDataCloudReader)
            mock_reader_config = MagicMock()
            mock_reader_config.to_object.return_value = mock_reader
            mock_reader_config.force = False

            mock_writer = MagicMock(spec=BaseDataCloudWriter)
            mock_writer_config = MagicMock()
            mock_writer_config.to_object.return_value = mock_writer
            mock_writer_config.force = False

            mock_spark_config = MagicMock(spec=SparkConfig)
            mock_config.spark_provider_config = None

            mock_config.reader_config = mock_reader_config
            mock_config.writer_config = mock_writer_config
            mock_config.spark_config = mock_spark_config

            client = Client()

            mock_get_session.assert_called_once_with(mock_spark_config)
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

    def test_read_dlo_deltas(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        reader.read_dlo_deltas.return_value = mock_df

        client = Client(reader=reader, writer=writer)
        result = client.read_dlo_deltas("test_dlo")

        reader.read_dlo_deltas.assert_called_once_with("test_dlo")
        assert result is mock_df
        assert "test_dlo" in client._data_layer_history[DataCloudObjectType.DLO]

    def test_read_dmo_deltas(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        reader.read_dmo_deltas.return_value = mock_df

        client = Client(reader=reader, writer=writer)
        result = client.read_dmo_deltas("test_dmo")

        reader.read_dmo_deltas.assert_called_once_with("test_dmo")
        assert result is mock_df
        assert "test_dmo" in client._data_layer_history[DataCloudObjectType.DMO]

    def test_write_dlo_deltas(self, reset_client, mock_spark):
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)
        mock_query = MagicMock()
        writer.write_dlo_deltas.return_value = mock_query

        client = Client(reader=reader, writer=writer)
        client._record_dlo_access("some_dlo")

        result = client.write_dlo_deltas(
            "test_dlo", mock_df, WriteMode.APPEND, extra_param=True
        )

        writer.write_dlo_deltas.assert_called_once_with(
            "test_dlo", mock_df, WriteMode.APPEND, extra_param=True
        )
        assert result is mock_query

    def test_write_dlo_deltas_after_dmo_read_raises_exception(
        self, reset_client, mock_spark
    ):
        """Streaming DLO write is subject to the same DLO/DMO mixing guard."""
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        mock_df = MagicMock(spec=DataFrame)

        client = Client(reader=reader, writer=writer)
        client._record_dmo_access("test_dmo")

        with pytest.raises(DataCloudAccessLayerException) as exc_info:
            client.write_dlo_deltas("test_dlo", mock_df, WriteMode.APPEND)

        assert "test_dmo" in str(exc_info.value)
        writer.write_dlo_deltas.assert_not_called()

    def test_streaming_read_write_flow(self, reset_client, mock_spark):
        """A read_dlo_deltas → write_dlo_deltas flow stays within the DLO layer."""
        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        stream_df = MagicMock(spec=DataFrame)
        reader.read_dlo_deltas.return_value = stream_df

        client = Client(reader=reader, writer=writer)

        df = client.read_dlo_deltas("source_dll")
        client.write_dlo_deltas("target_dll", df, WriteMode.MERGE_UPSERT_DELETE)

        reader.read_dlo_deltas.assert_called_once_with("source_dll")
        writer.write_dlo_deltas.assert_called_once_with(
            "target_dll", stream_df, WriteMode.MERGE_UPSERT_DELETE
        )
        assert "source_dll" in client._data_layer_history[DataCloudObjectType.DLO]

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


class TestClientLlmGatewayGenerateText:

    @patch("datacustomcode.client._build_spark_llm_gateway")
    def test_forwards_args_to_spark_llm_gateway(self, mock_build_gateway, reset_client):
        mock_spark_gateway = MagicMock()
        mock_spark_gateway.llm_gateway_generate_text.return_value = "reply"
        mock_build_gateway.return_value = mock_spark_gateway

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(reader=reader, writer=writer)

        result = client.llm_gateway_generate_text("ping", model_id="test-model")

        assert result == "reply"
        mock_spark_gateway.llm_gateway_generate_text.assert_called_once_with(
            "ping", model_id="test-model"
        )

    @patch("datacustomcode.client._build_spark_llm_gateway")
    def test_gateway_is_built_lazily_and_cached(self, mock_build_gateway, reset_client):
        mock_spark_gateway = MagicMock()
        mock_spark_gateway.llm_gateway_generate_text.return_value = "ok"
        mock_build_gateway.return_value = mock_spark_gateway

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(reader=reader, writer=writer)

        mock_build_gateway.assert_not_called()

        client.llm_gateway_generate_text("a")
        client.llm_gateway_generate_text("b")

        mock_build_gateway.assert_called_once_with()
        assert mock_spark_gateway.llm_gateway_generate_text.call_count == 2

    @patch("datacustomcode.client._build_spark_llm_gateway")
    def test_uses_injected_spark_llm_gateway_without_config_lookup(
        self, mock_build_gateway, reset_client
    ):
        injected = MagicMock()
        injected.llm_gateway_generate_text.return_value = "from-injected"

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(reader=reader, writer=writer, spark_llm_gateway=injected)

        result = client.llm_gateway_generate_text("hello")

        assert result == "from-injected"
        injected.llm_gateway_generate_text.assert_called_once_with(
            "hello", model_id=None
        )
        mock_build_gateway.assert_not_called()


class TestLLMGatewayGenerateTextCol:
    """The module-level ``llm_gateway_generate_text_col`` is a thin wrapper
    that resolves the client-owned :class:`SparkLLMGateway` and delegates.
    """

    @patch("datacustomcode.client._build_spark_llm_gateway")
    def test_delegates_to_spark_llm_gateway(self, mock_build_gateway):
        mock_spark_gateway = MagicMock()
        sentinel_col = MagicMock(name="col")
        mock_spark_gateway.llm_gateway_generate_text_col.return_value = sentinel_col
        mock_build_gateway.return_value = mock_spark_gateway

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        Client(reader=reader, writer=writer)

        values = {"name": MagicMock()}
        result = llm_gateway_generate_text_col("Greet {name}", values, model_id="m")

        assert result is sentinel_col
        mock_spark_gateway.llm_gateway_generate_text_col.assert_called_once_with(
            "Greet {name}", values, model_id="m"
        )


class TestClientEinsteinPredict:

    @patch("datacustomcode.client._build_spark_einstein_predictions")
    def test_forwards_args_to_spark_predictions(self, mock_build, reset_client):
        mock_predictions = MagicMock()
        mock_predictions.einstein_predict.return_value = {"results": [1]}
        mock_build.return_value = mock_predictions

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(reader=reader, writer=writer)

        result = client.einstein_predict(
            "model1", PredictionType.REGRESSION, {"beds": 3}, settings={"k": 1}
        )

        assert result == {"results": [1]}
        mock_predictions.einstein_predict.assert_called_once_with(
            "model1", PredictionType.REGRESSION, {"beds": 3}, settings={"k": 1}
        )

    @patch("datacustomcode.client._build_spark_einstein_predictions")
    def test_predictions_built_lazily_and_cached(self, mock_build, reset_client):
        mock_predictions = MagicMock()
        mock_predictions.einstein_predict.return_value = {}
        mock_build.return_value = mock_predictions

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(reader=reader, writer=writer)

        mock_build.assert_not_called()

        client.einstein_predict("m", PredictionType.REGRESSION, {"a": 1})
        client.einstein_predict("m", PredictionType.REGRESSION, {"b": 2})

        mock_build.assert_called_once_with()
        assert mock_predictions.einstein_predict.call_count == 2

    @patch("datacustomcode.client._build_spark_einstein_predictions")
    def test_uses_injected_spark_predictions_without_config_lookup(
        self, mock_build, reset_client
    ):
        injected = MagicMock()
        injected.einstein_predict.return_value = {"from": "injected"}

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        client = Client(
            reader=reader, writer=writer, spark_einstein_predictions=injected
        )

        result = client.einstein_predict("m", PredictionType.REGRESSION, {"a": 1})

        assert result == {"from": "injected"}
        injected.einstein_predict.assert_called_once_with(
            "m", PredictionType.REGRESSION, {"a": 1}, settings=None
        )
        mock_build.assert_not_called()


class TestEinsteinPredictCol:
    """The module-level ``einstein_predict_col`` is a thin wrapper that resolves
    the client-owned :class:`SparkEinsteinPredictions` and delegates.
    """

    @patch("datacustomcode.client._build_spark_einstein_predictions")
    def test_delegates_to_spark_predictions(self, mock_build):
        mock_predictions = MagicMock()
        sentinel_col = MagicMock(name="col")
        mock_predictions.einstein_predict_col.return_value = sentinel_col
        mock_build.return_value = mock_predictions

        reader = MagicMock(spec=BaseDataCloudReader)
        writer = MagicMock(spec=BaseDataCloudWriter)
        Client(reader=reader, writer=writer)

        features = {"beds": MagicMock()}
        result = einstein_predict_col(
            "model1", PredictionType.REGRESSION, features, settings={"k": 1}
        )

        assert result is sentinel_col
        mock_predictions.einstein_predict_col.assert_called_once_with(
            "model1", PredictionType.REGRESSION, features, settings={"k": 1}
        )


# Add tests for DefaultSparkSessionProvider
class TestDefaultSparkSessionProvider:

    @patch("pyspark.sql.SparkSession")
    def test_get_session_with_master(self, mock_spark_session):
        """Test DefaultSparkSessionProvider with master specified"""
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

        from datacustomcode.spark.default import DefaultSparkSessionProvider

        provider = DefaultSparkSessionProvider()
        result = provider.get_session(spark_config)

        mock_builder.master.assert_called_once_with("local[1]")
        mock_master_builder.appName.assert_called_once_with("test-app")
        mock_app_name_builder.config.assert_called_once_with(
            "spark.executor.memory", "1g"
        )
        mock_config_builder.getOrCreate.assert_called_once()
        assert result is mock_session

    @patch("pyspark.sql.SparkSession")
    def test_get_session_with_multiple_options(self, mock_spark_session):
        """Test DefaultSparkSessionProvider with multiple config options"""
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

        from datacustomcode.spark.default import DefaultSparkSessionProvider

        provider = DefaultSparkSessionProvider()
        result = provider.get_session(spark_config)

        mock_builder.appName.assert_called_once_with("test-app")

        # Check config was called for each option (order not guaranteed)
        assert mock_app_name_builder.config.call_count == 1
        assert mock_config_builder1.config.call_count == 1
        assert mock_config_builder2.config.call_count == 1

        mock_config_builder3.getOrCreate.assert_called_once()
        assert result is mock_session
