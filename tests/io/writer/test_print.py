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
    def mock_reader(self):
        """Create a mock QueryAPIDataCloudReader."""
        reader = MagicMock()
        mock_dlo_df = MagicMock()
        mock_dlo_df.columns = ["col1", "col2"]
        reader.read_dlo.return_value = mock_dlo_df
        return reader

    @pytest.fixture
    def print_writer(self, mock_spark_session, mock_reader):
        """Create a PrintDataCloudWriter instance."""
        return PrintDataCloudWriter(mock_spark_session, mock_reader)

    def test_write_to_dlo(self, print_writer, mock_dataframe):
        """Test write_to_dlo method calls dataframe.show()."""
        # Mock the validate_dataframe_columns_against_dlo method
        print_writer.validate_dataframe_columns_against_dlo = MagicMock()

        # Call the method
        print_writer.write_to_dlo("test_dlo", mock_dataframe, WriteMode.OVERWRITE)

        # Verify show() was called
        mock_dataframe.show.assert_called_once()

        # Verify validate_dataframe_columns_against_dlo was called
        print_writer.validate_dataframe_columns_against_dlo.assert_called_once()

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
            # Mock the validate_dataframe_columns_against_dlo method
            print_writer.validate_dataframe_columns_against_dlo = MagicMock()
            # Call method
            print_writer.write_to_dlo(name, mock_dataframe, write_mode)

            # Verify show() was called with no arguments
            mock_dataframe.show.assert_called_once_with()

            print_writer.validate_dataframe_columns_against_dlo.assert_called_once()

    def test_validate_dataframe_columns_against_dlo(self, print_writer, mock_dataframe):
        """Test validate_dataframe_columns_against_dlo method."""
        # Mock the QueryAPIDataCloudReader

        # Set up mock dataframe columns
        mock_dataframe.columns = ["col1", "col2", "col3"]

        # Test that validation raises ValueError for extra columns
        with pytest.raises(ValueError) as exc_info:
            print_writer.validate_dataframe_columns_against_dlo(
                mock_dataframe, "test_dlo"
            )

        assert "col3" in str(exc_info.value)

        # Test successful validation with matching columns
        mock_dataframe.columns = ["col1", "col2"]
        print_writer.validate_dataframe_columns_against_dlo(mock_dataframe, "test_dlo")
