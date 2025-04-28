from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
import yaml

from datacustomcode.config import (
    AccessLayerObjectConfig,
    ClientConfig,
    SparkConfig,
    _defaults,
    config,
)
from datacustomcode.io.base import BaseDataAccessLayer
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter


class MockDataCloudReader(BaseDataCloudReader):
    """Mock reader for testing."""

    CONFIG_NAME = "MockDataCloudReader_config"

    def read_dlo(self, name: str):
        return MagicMock()

    def read_dmo(self, name: str):
        return MagicMock()


class MockDataCloudWriter(BaseDataCloudWriter):
    """Mock writer for testing."""

    CONFIG_NAME = "MockDataCloudWriter_config"

    def write_to_dlo(self, name, dataframe, write_mode):
        pass

    def write_to_dmo(self, name, dataframe, write_mode):
        pass


class TestSparkConfig:
    def test_initialization(self):
        config = SparkConfig(app_name="test-app")
        assert config.app_name == "test-app"
        assert config.master is None
        assert config.options == {}

        config = SparkConfig(
            app_name="test-app",
            master="local[1]",
            options={"spark.executor.memory": "1g"},
            force=True,
        )
        assert config.app_name == "test-app"
        assert config.master == "local[1]"
        assert config.options == {"spark.executor.memory": "1g"}
        assert config.force is True


class TestAccessLayerObjectConfig:
    def test_initialization(self):
        config = AccessLayerObjectConfig(
            type_config_name="MockDataCloudReader_config",
            options={"option1": "value1"},
            force=True,
        )
        assert config.type_config_name == "MockDataCloudReader_config"
        assert config.options == {"option1": "value1"}
        assert config.force is True

    @patch.object(BaseDataAccessLayer, "subclass_from_config_name")
    def test_to_object(self, mock_subclass):
        mock_class = MagicMock()
        mock_instance = MagicMock(spec=BaseDataCloudReader)
        mock_class.return_value = mock_instance
        mock_subclass.return_value = mock_class

        spark = MagicMock(spec=SparkSession)
        config = AccessLayerObjectConfig(
            type_config_name="MockDataCloudReader_config",
            options={"option1": "value1"},
        )

        result = config.to_object(spark)

        mock_subclass.assert_called_once_with("MockDataCloudReader_config")
        mock_class.assert_called_once_with(spark=spark, option1="value1")
        assert result is mock_instance


class TestClientConfig:
    def test_initialization(self):
        config = ClientConfig()
        assert config.reader_config is None
        assert config.writer_config is None
        assert config.spark_config is None

        reader_config = AccessLayerObjectConfig(
            type_config_name="MockDataCloudReader_config",
        )
        writer_config = AccessLayerObjectConfig(
            type_config_name="MockDataCloudWriter_config",
        )
        spark_config = SparkConfig(app_name="test-app")

        config = ClientConfig(
            reader_config=reader_config,
            writer_config=writer_config,
            spark_config=spark_config,
        )

        assert config.reader_config.type_config_name == reader_config.type_config_name
        assert config.reader_config.options == reader_config.options
        assert config.reader_config.force == reader_config.force

        assert config.writer_config.type_config_name == writer_config.type_config_name
        assert config.writer_config.options == writer_config.options
        assert config.writer_config.force == writer_config.force

        assert config.spark_config.app_name == spark_config.app_name
        assert config.spark_config.master == spark_config.master
        assert config.spark_config.options == spark_config.options
        assert config.spark_config.force == spark_config.force

    def test_update_without_force(self):
        base_config = ClientConfig(
            reader_config=AccessLayerObjectConfig(
                type_config_name="BaseReader",
                options={},
                force=False,
            ),
            writer_config=AccessLayerObjectConfig(
                type_config_name="BaseWriter",
                options={},
                force=False,
            ),
            spark_config=SparkConfig(
                app_name="base-app",
                master="local[1]",
                force=False,
            ),
        )

        other_config = ClientConfig(
            reader_config=AccessLayerObjectConfig(
                type_config_name="OtherReader",
                options={},
                force=False,
            ),
            writer_config=AccessLayerObjectConfig(
                type_config_name="OtherWriter",
                options={},
                force=False,
            ),
            spark_config=SparkConfig(
                app_name="other-app",
                master="local[2]",
                force=False,
            ),
        )

        result = base_config.update(other_config)

        # Other config should take precedence
        assert result.reader_config.type_config_name == "OtherReader"
        assert result.writer_config.type_config_name == "OtherWriter"
        assert result.spark_config.app_name == "other-app"
        assert result.spark_config.master == "local[2]"

    def test_update_with_force(self):
        base_config = ClientConfig(
            reader_config=AccessLayerObjectConfig(
                type_config_name="BaseReader",
                options={},
                force=True,  # Force on base reader
            ),
            writer_config=None,
            spark_config=SparkConfig(
                app_name="base-app",
                master="local[1]",
                force=False,
            ),
        )

        other_config = ClientConfig(
            reader_config=AccessLayerObjectConfig(
                type_config_name="OtherReader",
                options={},
                force=False,
            ),
            writer_config=AccessLayerObjectConfig(
                type_config_name="OtherWriter",
                options={},
                force=True,  # Force on other writer
            ),
            spark_config=SparkConfig(
                app_name="other-app",
                master="local[2]",
                force=True,  # Force on other spark
            ),
        )

        result = base_config.update(other_config)

        # Forced configs should take precedence
        assert result.reader_config.type_config_name == "BaseReader"  # Base is forced
        assert result.writer_config.type_config_name == "OtherWriter"  # Other is forced
        assert result.spark_config.app_name == "other-app"  # Other is forced
        assert result.spark_config.master == "local[2]"  # Other is forced

    def test_update_with_missing_configs(self):
        base_config = ClientConfig(
            reader_config=AccessLayerObjectConfig(
                type_config_name="BaseReader",
                options={},
                force=False,
            ),
            writer_config=None,
            spark_config=None,
        )

        other_config = ClientConfig(
            reader_config=None,
            writer_config=AccessLayerObjectConfig(
                type_config_name="OtherWriter",
                options={},
                force=False,
            ),
            spark_config=SparkConfig(
                app_name="other-app",
                master="local[2]",
                force=False,
            ),
        )

        result = base_config.update(other_config)

        # Configs should be merged with non-None values taking precedence
        assert result.reader_config.type_config_name == "BaseReader"
        assert result.writer_config.type_config_name == "OtherWriter"
        assert result.spark_config.app_name == "other-app"
        assert result.spark_config.master == "local[2]"


class TestConfigLoading:
    def test_load_config_from_file(self):
        # Create a temporary config file
        config_data = {
            "reader_config": {
                "type_config_name": "MockDataCloudReader",
                "options": {"option1": "value1"},
                "force": False,
            },
            "writer_config": {
                "type_config_name": "MockDataCloudWriter",
                "options": {"option2": "value2"},
                "force": True,
            },
            "spark_config": {
                "app_name": "test-app",
                "master": "local[2]",
                "options": {"spark.executor.memory": "2g"},
                "force": False,
            },
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as temp:
            yaml.dump(config_data, temp)
            temp_path = temp.name

        try:
            # Create a new ClientConfig and load from file
            empty_config = ClientConfig()
            result = empty_config.load(temp_path)

            # Verify the loaded config
            assert result.reader_config.type_config_name == "MockDataCloudReader"
            assert result.reader_config.options == {"option1": "value1"}
            assert result.reader_config.force is False

            assert result.writer_config.type_config_name == "MockDataCloudWriter"
            assert result.writer_config.options == {"option2": "value2"}
            assert result.writer_config.force is True

            assert result.spark_config.app_name == "test-app"
            assert result.spark_config.master == "local[2]"
            assert result.spark_config.options == {"spark.executor.memory": "2g"}
            assert result.spark_config.force is False
        finally:
            # Clean up the temporary file
            os.unlink(temp_path)

    def test_defaults(self):
        # Just verify that _defaults function exists and returns a string path
        result = _defaults()
        assert isinstance(result, str)
        assert result.endswith("config.yaml")


def test_global_config_initialization():
    # The module-level config object should be initialized
    assert config is not None
    # It should be an instance of ClientConfig
    assert isinstance(config, ClientConfig)
