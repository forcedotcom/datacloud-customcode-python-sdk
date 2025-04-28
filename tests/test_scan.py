from __future__ import annotations

import os
import tempfile
import textwrap

import pytest

from datacustomcode.scan import (
    DataAccessLayerCalls,
    dc_config_json_from_file,
    scan_file,
)


def create_test_script(content: str) -> str:
    """Create a temporary Python file with the given content."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp:
        temp.write(content)
        temp_path = temp.name
    return temp_path


class TestClientMethodVisitor:
    def test_variable_tracking(self):
        """Test that the visitor can track variable assignments."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            dlo_name = "my_dlo"
            dmo_name = "my_dmo"
            client.read_dlo(dlo_name)
            # Don't mix with DMO reads in the same test
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "my_dlo" in result.read_dlo
            assert len(result.read_dmo) == 0  # No DMO reads
        finally:
            os.unlink(temp_path)

    def test_string_literals(self):
        """Test that the visitor can track string literals in method calls."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            client.read_dlo("direct_dlo_1")
            client.read_dlo("direct_dlo_2")
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "direct_dlo_1" in result.read_dlo
            assert "direct_dlo_2" in result.read_dlo
        finally:
            os.unlink(temp_path)

    def test_cannot_mix_dlo_dmo_reads(self):
        """Test that mixing DLO and DMO reads in the same file raises an error."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            client.read_dlo("direct_dlo")
            client.read_dmo("direct_dmo")
        """
        )
        temp_path = create_test_script(content)
        try:
            # This should raise a validation error due to mixing DLO and DMO reads
            with pytest.raises(
                Exception, match="Cannot read from DLO and DMO in the same file"
            ):
                scan_file(temp_path)
        finally:
            os.unlink(temp_path)

    def test_read_both_dlo_dmo_throws_error(self):
        """Test that reading from both DLO and DMO throws an error."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            # Read from both DLO and DMO in the same file
            df1 = client.read_dlo("some_dlo")
            df2 = client.read_dmo("some_dmo")

            # This operation should never happen as validation should fail
            result = df1.join(df2, "key")
            client.write_to_dlo("output", result, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            with pytest.raises(
                ValueError, match="Cannot read from DLO and DMO in the same file"
            ):
                scan_file(temp_path)
        finally:
            os.unlink(temp_path)


class TestScanFile:
    def test_valid_dlo_to_dlo(self):
        """Test scanning a file with valid DLO to DLO operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DLO
            df = client.read_dlo("input_dlo")

            # Transform data
            df = df.filter(df.col > 10)

            # Write to DLO
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "input_dlo" in result.read_dlo
            assert "output_dlo" in result.write_to_dlo
            assert len(result.read_dmo) == 0
            assert len(result.write_to_dmo) == 0
        finally:
            os.unlink(temp_path)

    def test_valid_dmo_to_dmo(self):
        """Test scanning a file with valid DMO to DMO operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DMO
            df = client.read_dmo("input_dmo")

            # Transform data
            df = df.filter(df.col > 10)

            # Write to DMO
            client.write_to_dmo("output_dmo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "input_dmo" in result.read_dmo
            assert "output_dmo" in result.write_to_dmo
            assert len(result.read_dlo) == 0
            assert len(result.write_to_dlo) == 0
        finally:
            os.unlink(temp_path)

    def test_multiple_reads(self):
        """Test scanning a file with multiple read operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from multiple DLOs
            df1 = client.read_dlo("input_dlo_1")
            df2 = client.read_dlo("input_dlo_2")

            # Join dataframes
            result_df = df1.join(df2, "key_column")

            # Write to DLO
            client.write_to_dlo("output_dlo", result_df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "input_dlo_1" in result.read_dlo
            assert "input_dlo_2" in result.read_dlo
            assert "output_dlo" in result.write_to_dlo
            assert len(result.read_dmo) == 0
            assert len(result.write_to_dmo) == 0
        finally:
            os.unlink(temp_path)

    def test_invalid_mix_dlo_dmo(self):
        """Test scanning a file with invalid mix of DLO and DMO operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DLO
            df = client.read_dlo("input_dlo")

            # Write to DMO - invalid operation
            client.write_to_dmo("output_dmo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            with pytest.raises(
                ValueError, match="Cannot read from DLO and write to DMO"
            ):
                scan_file(temp_path)
        finally:
            os.unlink(temp_path)

    def test_read_dmo_write_dlo_throws_error(self):
        """Test that reading from DMO and writing to DLO throws an error."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DMO
            df = client.read_dmo("input_dmo")

            # Write to DLO - invalid operation
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            with pytest.raises(
                ValueError, match="Cannot read from DMO and write to DLO"
            ):
                scan_file(temp_path)
        finally:
            os.unlink(temp_path)

    def test_invalid_multiple_writes(self):
        """Test scanning a file with multiple write operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DLO
            df = client.read_dlo("input_dlo")

            # Write to multiple DLOs - invalid
            client.write_to_dlo("output_dlo_1", df, "overwrite")
            client.write_to_dlo("output_dlo_2", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            with pytest.raises(ValueError, match="Cannot write to more than one DLO"):
                scan_file(temp_path)
        finally:
            os.unlink(temp_path)


class TestDcConfigJson:
    def test_dlo_to_dlo_config(self):
        """Test generating config JSON for DLO to DLO operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DLO
            df = client.read_dlo("input_dlo")

            # Write to DLO
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            config = dc_config_json_from_file(temp_path)
            assert config["permissions"]["read"]["dlo"] == ["input_dlo"]
            assert config["permissions"]["write"]["dlo"] == ["output_dlo"]
            assert "dmo" not in config["permissions"]["read"]
            assert "dmo" not in config["permissions"]["write"]
            assert config["entryPoint"] == os.path.basename(temp_path)
        finally:
            os.unlink(temp_path)

    def test_dmo_to_dmo_config(self):
        """Test generating config JSON for DMO to DMO operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DMO
            df = client.read_dmo("input_dmo")

            # Write to DMO
            client.write_to_dmo("output_dmo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            config = dc_config_json_from_file(temp_path)
            assert config["permissions"]["read"]["dmo"] == ["input_dmo"]
            assert config["permissions"]["write"]["dmo"] == ["output_dmo"]
            assert "dlo" not in config["permissions"]["read"]
            assert "dlo" not in config["permissions"]["write"]
            assert config["entryPoint"] == os.path.basename(temp_path)
        finally:
            os.unlink(temp_path)


class TestDataAccessLayerCalls:
    """Tests for the DataAccessLayerCalls class directly."""

    def test_input_str_output_str_properties(self):
        """Test the input_str and output_str properties."""
        # Test with DLO
        dlo_calls = DataAccessLayerCalls(
            read_dlo=frozenset(["input_dlo"]),
            read_dmo=frozenset(),
            write_to_dlo=frozenset(["output_dlo"]),
            write_to_dmo=frozenset(),
        )
        assert dlo_calls.input_str == "input_dlo"
        assert dlo_calls.output_str == "output_dlo"

        # Test with DMO
        dmo_calls = DataAccessLayerCalls(
            read_dlo=frozenset(),
            read_dmo=frozenset(["input_dmo"]),
            write_to_dlo=frozenset(),
            write_to_dmo=frozenset(["output_dmo"]),
        )
        assert dmo_calls.input_str == "input_dmo"
        assert dmo_calls.output_str == "output_dmo"


def test_real_world_example():
    """Test scanning a more complex, real-world example script."""
    content = textwrap.dedent(
        """
        from datacustomcode.client import Client
        from pyspark.sql.functions import col, expr

        # Constants
        SOURCE_DLO = "customer_data_raw"
        TARGET_DLO = "customer_data_enriched"

        def process_customer_data():
            # Initialize client
            client = Client()

            # Read data from DLO
            customer_df = client.read_dlo(SOURCE_DLO)

            # Perform transformations
            enriched_df = (
                customer_df
                .filter(col("age") > 18)
                .withColumn("full_name", expr("concat(first_name, ' ', last_name)"))
                .withColumn("age_group", expr("case when age < 30 then 'Young' " +
                                             "when age < 60 then 'Middle-aged' " +
                                             "else 'Senior' end"))
                .drop("first_name", "last_name")
            )

            # Write transformed data back to a different DLO
            client.write_to_dlo(TARGET_DLO, enriched_df, "overwrite")

            return "Processing complete"

        if __name__ == "__main__":
            process_customer_data()
    """
    )
    temp_path = create_test_script(content)
    try:
        result = scan_file(temp_path)
        assert "customer_data_raw" in result.read_dlo
        assert "customer_data_enriched" in result.write_to_dlo

        config = dc_config_json_from_file(temp_path)
        assert config["permissions"]["read"]["dlo"] == ["customer_data_raw"]
        assert config["permissions"]["write"]["dlo"] == ["customer_data_enriched"]
    finally:
        os.unlink(temp_path)
