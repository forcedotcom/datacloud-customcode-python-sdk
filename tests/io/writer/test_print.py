from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datacustomcode.io.writer.base import WriteMode
from datacustomcode.io.writer.print import PrintDataCloudWriter


class TestPrintDataCloudWriter:
    @pytest.fixture
    def mock_spark_session(self):
        """Create a mock Spark session."""
        spark = MagicMock()
        return spark

    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock PySpark DataFrame."""
        df = MagicMock()
        df.show = MagicMock()
        return df

    @pytest.fixture
    def print_writer(self, mock_spark_session):
        """Create a PrintDataCloudWriter instance."""
        return PrintDataCloudWriter(mock_spark_session)

    def test_write_to_dlo(self, print_writer, mock_dataframe):
        """Test write_to_dlo method calls dataframe.show()."""
        # Call the method
        print_writer.write_to_dlo("test_dlo", mock_dataframe, WriteMode.OVERWRITE)

        # Verify show() was called
        mock_dataframe.show.assert_called_once()

    def test_write_to_dmo(self, print_writer, mock_dataframe):
        """Test write_to_dmo method calls dataframe.show()."""
        # Call the method
        print_writer.write_to_dmo("test_dmo", mock_dataframe, WriteMode.OVERWRITE)

        # Verify show() was called
        mock_dataframe.show.assert_called_once()

    def test_config_name(self):
        """Test that the CONFIG_NAME class variable is set correctly."""
        assert PrintDataCloudWriter.CONFIG_NAME == "PrintDataCloudWriter"

    def test_ignores_name_and_write_mode(self, print_writer, mock_dataframe):
        """Test that the name and write_mode parameters are ignored."""
        # Call with different names and write modes
        test_cases = [
            ("name1", WriteMode.OVERWRITE),
            ("name2", WriteMode.APPEND),
            ("name3", WriteMode.MERGE),
        ]

        for name, write_mode in test_cases:
            # Reset mock before each call
            mock_dataframe.show.reset_mock()

            # Call method
            print_writer.write_to_dlo(name, mock_dataframe, write_mode)

            # Verify show() was called with no arguments
            mock_dataframe.show.assert_called_once_with()
