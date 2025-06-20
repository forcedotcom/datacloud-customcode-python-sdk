from __future__ import annotations

from unittest.mock import (
    MagicMock,
    PropertyMock,
    patch,
)

import pandas as pd
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import pytest

from datacustomcode.io.reader.query_api import (
    SQL_QUERY_TEMPLATE,
    QueryAPIDataCloudReader,
    _pandas_to_spark_schema,
)


class TestPandasToSparkSchema:
    def test_pandas_to_spark_schema(self):
        """Test conversion of pandas DataFrame schema to Spark schema."""
        # Create a test pandas DataFrame with various types
        data = {
            "string_col": ["a", "b"],
            "int_col": [1, 2],
            "float_col": [1.0, 2.0],
            "bool_col": [True, False],
        }
        df = pd.DataFrame(data)

        # Convert to Spark schema
        schema = _pandas_to_spark_schema(df)

        # Verify the schema
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4

        # Check each column's type mapping
        field_dict = {field.name: field for field in schema.fields}
        assert isinstance(field_dict["string_col"].dataType, StringType)
        assert isinstance(field_dict["int_col"].dataType, LongType)
        assert isinstance(field_dict["float_col"].dataType, DoubleType)
        assert isinstance(field_dict["bool_col"].dataType, BooleanType)
        assert all(field.nullable for field in schema.fields)

    def test_pandas_to_spark_schema_nullable(self):
        """Test setting nullable parameter in schema conversion."""
        df = pd.DataFrame({"col": [1, 2]})

        # Test with nullable=False
        schema = _pandas_to_spark_schema(df, nullable=False)
        assert not schema.fields[0].nullable

    def test_pandas_to_spark_schema_datetime_types(self):
        """Test conversion of pandas datetime types to Spark TimestampType."""
        import numpy as np

        # Create test data with different datetime types
        data = {
            "datetime_ns": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-02 11:00:00"]),
            "datetime_ns_utc": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-02 11:00:00"], utc=True),
            "datetime_ms": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-02 11:00:00"]).astype("datetime64[ms]"),
            "datetime_ms_utc": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-02 11:00:00"], utc=True).tz_localize(None).astype("datetime64[ms]"),
        }
        df = pd.DataFrame(data)

        # Convert to Spark schema
        schema = _pandas_to_spark_schema(df)

        # Verify the schema
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4

        # Check that all datetime columns map to TimestampType
        field_dict = {field.name: field for field in schema.fields}
        for field_name in ["datetime_ns", "datetime_ns_utc", "datetime_ms", "datetime_ms_utc"]:
            assert isinstance(field_dict[field_name].dataType, TimestampType), \
                f"Field {field_name} should be TimestampType, got {type(field_dict[field_name].dataType)}"
            assert field_dict[field_name].nullable

        # Verify the actual pandas dtypes to ensure our test data has the expected types
        assert str(df["datetime_ns"].dtype) == "datetime64[ns]"
        assert str(df["datetime_ns_utc"].dtype) == "datetime64[ns, UTC]"
        assert str(df["datetime_ms"].dtype) == "datetime64[ms]"
        assert str(df["datetime_ms_utc"].dtype) == "datetime64[ms]"


# Completely isolated test class for QueryAPIDataCloudReader
@pytest.mark.usefixtures("patch_all_requests")
class TestQueryAPIDataCloudReader:
    # Test-level patch to prevent any HTTP requests during the entire test class
    @pytest.fixture(scope="class", autouse=True)
    def patch_all_requests(self, request):
        """Patch all potential HTTP request methods to prevent real network calls."""
        patches = []

        # Patch requests methods
        for target in [
            "requests.get",
            "requests.post",
            "requests.session",
            "requests.adapters.HTTPAdapter.send",
            "urllib3.connectionpool.HTTPConnectionPool.urlopen",
        ]:
            patcher = patch(target)
            patches.append(patcher)
            patcher.start()

        def fin():
            for patcher in patches:
                patcher.stop()

        request.addfinalizer(fin)

    @pytest.fixture
    def mock_spark_session(self):
        """Create a mock Spark session."""
        spark = MagicMock()
        # Setup createDataFrame to return itself for chaining
        spark.createDataFrame.return_value = spark
        return spark

    @pytest.fixture
    def mock_pandas_dataframe(self):
        """Create a sample pandas DataFrame for testing."""
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    @pytest.fixture
    def mock_connection(self, mock_pandas_dataframe):
        """Create a completely mocked SalesforceCDPConnection."""
        mock_conn = MagicMock()
        mock_conn.get_pandas_dataframe.return_value = mock_pandas_dataframe

        # Ensure no real authentication happens
        mock_auth = MagicMock()
        mock_auth.get_token.return_value = ("fake_token", "fake_instance_url")
        type(mock_conn).authentication_helper = PropertyMock(return_value=mock_auth)

        return mock_conn

    @pytest.fixture
    def reader_without_init(self, mock_spark_session):
        """
        Special fixture that creates a partially-constructed reader instance.
        This avoids calling __init__ which tries to make connections.
        """
        with patch.object(QueryAPIDataCloudReader, "__init__", return_value=None):
            reader = QueryAPIDataCloudReader(None)  # None is ignored due to mock
            reader.spark = mock_spark_session
            yield reader

    def test_pandas_to_spark_schema_function(self):
        """Test the pandas to spark schema conversion function directly."""
        df = pd.DataFrame({"col": [1, 2]})
        schema = _pandas_to_spark_schema(df)
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 1

    def test_read_dlo(
        self, reader_without_init, mock_connection, mock_pandas_dataframe
    ):
        """Test read_dlo method."""
        # Set the mock connection on the reader
        reader_without_init._conn = mock_connection

        # Call read_dlo - this will use our mock connection now
        reader_without_init.read_dlo("test_dlo")

        # Verify get_pandas_dataframe was called with the right SQL
        mock_connection.get_pandas_dataframe.assert_called_once_with(
            SQL_QUERY_TEMPLATE.format("test_dlo", 1000)
        )

        # Verify DataFrame was created with auto-inferred schema
        reader_without_init.spark.createDataFrame.assert_called_once()
        args, _ = reader_without_init.spark.createDataFrame.call_args
        assert args[0] is mock_pandas_dataframe  # First arg is the pandas DataFrame
        assert isinstance(args[1], StructType)  # Second arg is the schema

    def test_read_dlo_with_schema(
        self, reader_without_init, mock_connection, mock_pandas_dataframe
    ):
        """Test read_dlo method with explicit schema."""
        # Set the mock connection on the reader
        reader_without_init._conn = mock_connection

        # Create custom schema
        custom_schema = StructType(
            [
                StructField("col1", LongType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        # Call read_dlo with schema
        reader_without_init.read_dlo("test_dlo", schema=custom_schema)

        # Verify get_pandas_dataframe was called with the right SQL
        mock_connection.get_pandas_dataframe.assert_called_once_with(
            SQL_QUERY_TEMPLATE.format("test_dlo", 1000)
        )

        # Verify DataFrame was created with provided schema
        reader_without_init.spark.createDataFrame.assert_called_once()
        args, _ = reader_without_init.spark.createDataFrame.call_args
        assert args[1] is custom_schema

    def test_read_dmo(
        self, reader_without_init, mock_connection, mock_pandas_dataframe
    ):
        """Test read_dmo method."""
        # Set the mock connection on the reader
        reader_without_init._conn = mock_connection

        # Call read_dmo
        reader_without_init.read_dmo("test_dmo")

        # Verify get_pandas_dataframe was called with the right SQL
        mock_connection.get_pandas_dataframe.assert_called_once_with(
            SQL_QUERY_TEMPLATE.format("test_dmo", 1000)
        )

        # Verify DataFrame was created
        reader_without_init.spark.createDataFrame.assert_called_once()
        args, _ = reader_without_init.spark.createDataFrame.call_args
        assert args[0] is mock_pandas_dataframe

    def test_read_dmo_with_schema(
        self, reader_without_init, mock_connection, mock_pandas_dataframe
    ):
        """Test read_dmo method with explicit schema."""
        # Set the mock connection on the reader
        reader_without_init._conn = mock_connection

        # Create custom schema
        custom_schema = StructType(
            [
                StructField("col1", LongType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        # Call read_dmo with schema
        reader_without_init.read_dmo("test_dmo", schema=custom_schema)

        # Verify get_pandas_dataframe was called with the right SQL
        mock_connection.get_pandas_dataframe.assert_called_once_with(
            SQL_QUERY_TEMPLATE.format("test_dmo", 1000)
        )

        # Verify DataFrame was created with provided schema
        reader_without_init.spark.createDataFrame.assert_called_once()
        args, _ = reader_without_init.spark.createDataFrame.call_args
        assert args[1] is custom_schema
