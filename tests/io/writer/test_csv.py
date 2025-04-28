from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest

from datacustomcode.io.writer.base import WriteMode
from datacustomcode.io.writer.csv import SUFFIX, CSVDataCloudWriter


class TestCSVDataCloudWriter:
    @pytest.fixture
    def mock_spark_session(self):
        """Create a mock Spark session."""
        spark = MagicMock()
        return spark

    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock PySpark DataFrame."""
        df = MagicMock()
        # Create a mock write property with a csv method
        write_mock = MagicMock()
        write_mock.csv = MagicMock()
        # Attach the write property to the dataframe
        type(df).write = PropertyMock(return_value=write_mock)
        return df

    @pytest.fixture
    def csv_writer(self, mock_spark_session):
        """Create a CSVDataCloudWriter instance with a mock SparkSession."""
        return CSVDataCloudWriter(mock_spark_session)

    def test_write_to_dlo(self, csv_writer, mock_dataframe):
        """Test write_to_dlo method."""
        # Test with different write modes
        test_cases = [
            ("test_dlo", WriteMode.OVERWRITE),
            ("another_dlo", WriteMode.APPEND),
            ("third_dlo", WriteMode.OVERWRITE_PARTITIONS),
            ("fourth_dlo", WriteMode.MERGE),
        ]

        for name, write_mode in test_cases:
            # Call the method
            csv_writer.write_to_dlo(name, mock_dataframe, write_mode)

            # Verify .csv() was called with correct parameters
            expected_name = f"{name}{SUFFIX}"
            mock_dataframe.write.csv.assert_called_with(expected_name, mode=write_mode)

            # Reset the mock for the next test case
            mock_dataframe.write.csv.reset_mock()

    def test_write_to_dmo(self, csv_writer, mock_dataframe):
        """Test write_to_dmo method."""
        # Test with different write modes
        test_cases = [
            ("test_dmo", WriteMode.OVERWRITE),
            ("another_dmo", WriteMode.APPEND),
            ("third_dmo", WriteMode.OVERWRITE_PARTITIONS),
            ("fourth_dmo", WriteMode.MERGE_UPSERT_DELETE),
        ]

        for name, write_mode in test_cases:
            # Call the method
            csv_writer.write_to_dmo(name, mock_dataframe, write_mode)

            # Verify .csv() was called with correct parameters
            expected_name = f"{name}{SUFFIX}"
            mock_dataframe.write.csv.assert_called_with(expected_name, mode=write_mode)

            # Reset the mock for the next test case
            mock_dataframe.write.csv.reset_mock()

    def test_suffix_is_applied(self, csv_writer, mock_dataframe):
        """Verify that the CSV suffix is correctly applied to the name."""
        name = "test_data"
        write_mode = WriteMode.OVERWRITE

        # Call both methods
        csv_writer.write_to_dlo(name, mock_dataframe, write_mode)
        csv_writer.write_to_dmo(name, mock_dataframe, write_mode)

        # Check that both calls used the name with suffix
        expected_name = f"{name}{SUFFIX}"
        assert mock_dataframe.write.csv.call_args_list[0][0][0] == expected_name
        assert mock_dataframe.write.csv.call_args_list[1][0][0] == expected_name

    def test_config_name(self):
        """Test that the CONFIG_NAME class variable is set correctly."""
        assert CSVDataCloudWriter.CONFIG_NAME == "CSVDataCloudWriter"

    def test_with_path_containing_extension(self, csv_writer, mock_dataframe):
        """Test behavior when name already contains a file extension."""
        # Test with a name that already contains .csv
        name_with_extension = "test_data.csv"
        write_mode = WriteMode.OVERWRITE

        # Call the method
        csv_writer.write_to_dlo(name_with_extension, mock_dataframe, write_mode)

        # Verify .csv() was called without adding another .csv extension
        mock_dataframe.write.csv.assert_called_with(
            name_with_extension, mode=write_mode
        )

        # Try with uppercase extension to make sure case insensitivity works
        name_with_uppercase_extension = "test_data.CSV"
        csv_writer.write_to_dmo(
            name_with_uppercase_extension, mock_dataframe, write_mode
        )
        mock_dataframe.write.csv.assert_called_with(
            name_with_uppercase_extension, mode=write_mode
        )

    def test_csv_options(self, mock_spark_session, mock_dataframe):
        """Test that CSV options are correctly passed to the writer."""
        # Extend the write mock to handle options chaining
        options_mock = MagicMock()
        options_mock.csv = MagicMock()
        mock_dataframe.write.option = MagicMock(return_value=options_mock)
        mock_dataframe.write.options = MagicMock(return_value=options_mock)

        # Create a writer with options
        csv_writer = CSVDataCloudWriter(mock_spark_session)

        name = "test_data"
        write_mode = WriteMode.OVERWRITE

        # Call the method
        csv_writer.write_to_dlo(name, mock_dataframe, write_mode)

        # Currently, options aren't supported, just basic behavior
        # If options were added, this test would need to be updated
        mock_dataframe.write.csv.assert_called_once()

        # A more complete implementation might:
        # 1. Support header option: mock_dataframe.write.option("header", "true")
        # 2. Support delimiter: mock_dataframe.write.option("delimiter", ",")
        # 3. Support various encoding options
