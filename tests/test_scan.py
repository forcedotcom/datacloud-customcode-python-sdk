from __future__ import annotations

import os
import tempfile
import textwrap
from unittest.mock import patch

import pytest

from datacustomcode.scan import (
    DataAccessLayerCalls,
    dc_config_json_from_file,
    scan_file,
    scan_file_for_imports,
    write_requirements_file,
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

    def test_multiple_writes(self):
        """Test scanning a file with multiple write operations."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()

            # Read from DLO
            df = client.read_dlo("input_dlo")

            # Transform data for different outputs
            df_filtered = df.filter(df.col > 10)
            df_aggregated = df.groupBy("category").agg({"value": "sum"})

            # Write to multiple DLOs
            client.write_to_dlo("output_filtered", df_filtered, "overwrite")
            client.write_to_dlo("output_aggregated", df_aggregated, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        try:
            result = scan_file(temp_path)
            assert "input_dlo" in result.read_dlo
            assert "output_filtered" in result.write_to_dlo
            assert "output_aggregated" in result.write_to_dlo
            assert len(result.read_dlo) == 1
            assert len(result.write_to_dlo) == 2
            assert len(result.read_dmo) == 0
            assert len(result.write_to_dmo) == 0
        finally:
            os.unlink(temp_path)


class TestDcConfigJson:
    @patch(
        "datacustomcode.scan.DATA_TRANSFORM_CONFIG_TEMPLATE",
        {
            "sdkVersion": "1.2.3",
            "entryPoint": "",
            "dataspace": "default",
            "permissions": {
                "read": {},
                "write": {},
            },
        },
    )
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
            result = dc_config_json_from_file(temp_path)
            assert result["entryPoint"] == os.path.basename(temp_path)
            assert result["dataspace"] == "default"
            assert result["sdkVersion"] == "1.2.3"  # From mocked version
            assert result["permissions"]["read"]["dlo"] == ["input_dlo"]
            assert result["permissions"]["write"]["dlo"] == ["output_dlo"]
        finally:
            os.remove(temp_path)

    @patch(
        "datacustomcode.scan.DATA_TRANSFORM_CONFIG_TEMPLATE",
        {
            "sdkVersion": "1.2.3",
            "entryPoint": "",
            "dataspace": "default",
            "permissions": {
                "read": {},
                "write": {},
            },
        },
    )
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
            assert config["entryPoint"] == os.path.basename(temp_path)
            assert config["dataspace"] == "default"
            assert config["sdkVersion"] == "1.2.3"  # From mocked version
            assert config["permissions"]["read"]["dmo"] == ["input_dmo"]
            assert config["permissions"]["write"]["dmo"] == ["output_dmo"]
        finally:
            os.remove(temp_path)

    @patch(
        "datacustomcode.scan.DATA_TRANSFORM_CONFIG_TEMPLATE",
        {
            "sdkVersion": "1.2.3",
            "entryPoint": "",
            "dataspace": "",
            "permissions": {
                "read": {},
                "write": {},
            },
        },
    )
    def test_preserves_existing_dataspace(self):
        """Test that existing dataspace value is preserved when config.json exists."""
        import json

        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            df = client.read_dlo("input_dlo")
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        file_dir = os.path.dirname(temp_path)
        config_path = os.path.join(file_dir, "config.json")

        try:
            # Create an existing config.json with a custom dataspace
            existing_config = {
                "sdkVersion": "1.0.0",
                "entryPoint": "test.py",
                "dataspace": "my_custom_dataspace",
                "permissions": {
                    "read": {"dlo": ["old_dlo"]},
                    "write": {"dlo": ["old_output"]},
                },
            }
            with open(config_path, "w") as f:
                json.dump(existing_config, f)

            # Generate new config - should preserve dataspace
            result = dc_config_json_from_file(temp_path)
            assert result["dataspace"] == "my_custom_dataspace"
            assert result["permissions"]["read"]["dlo"] == ["input_dlo"]
            assert result["permissions"]["write"]["dlo"] == ["output_dlo"]
        finally:
            os.remove(temp_path)
            if os.path.exists(config_path):
                os.remove(config_path)

    @patch(
        "datacustomcode.scan.DATA_TRANSFORM_CONFIG_TEMPLATE",
        {
            "sdkVersion": "1.2.3",
            "entryPoint": "",
            "dataspace": "",
            "permissions": {
                "read": {},
                "write": {},
            },
        },
    )
    def test_rejects_empty_dataspace(self):
        """Test that empty dataspace value uses default and logs error."""
        import json

        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            df = client.read_dlo("input_dlo")
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        file_dir = os.path.dirname(temp_path)
        config_path = os.path.join(file_dir, "config.json")

        try:
            # Create an existing config.json with empty dataspace
            existing_config = {
                "sdkVersion": "1.0.0",
                "entryPoint": "test.py",
                "dataspace": "",
                "permissions": {
                    "read": {"dlo": ["old_dlo"]},
                    "write": {"dlo": ["old_output"]},
                },
            }
            with open(config_path, "w") as f:
                json.dump(existing_config, f)

            # Should use "default" for empty dataspace (not raise error)
            result = dc_config_json_from_file(temp_path)
            assert result["dataspace"] == "default"
            assert result["permissions"]["read"]["dlo"] == ["input_dlo"]
            assert result["permissions"]["write"]["dlo"] == ["output_dlo"]
        finally:
            os.remove(temp_path)
            if os.path.exists(config_path):
                os.remove(config_path)

    @patch(
        "datacustomcode.scan.DATA_TRANSFORM_CONFIG_TEMPLATE",
        {
            "sdkVersion": "1.2.3",
            "entryPoint": "",
            "dataspace": "",
            "permissions": {
                "read": {},
                "write": {},
            },
        },
    )
    def test_rejects_missing_dataspace(self):
        """Test missing config.json uses default dataspace."""
        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            df = client.read_dlo("input_dlo")
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)

        try:
            # No existing config.json - should use "default" dataspace
            result = dc_config_json_from_file(temp_path)
            assert result["dataspace"] == "default"
            assert result["permissions"]["read"]["dlo"] == ["input_dlo"]
            assert result["permissions"]["write"]["dlo"] == ["output_dlo"]
        finally:
            os.remove(temp_path)

    def test_raises_error_on_invalid_json(self):
        """Test that invalid JSON in config.json raises an error."""

        content = textwrap.dedent(
            """
            from datacustomcode.client import Client

            client = Client()
            df = client.read_dlo("input_dlo")
            client.write_to_dlo("output_dlo", df, "overwrite")
        """
        )
        temp_path = create_test_script(content)
        file_dir = os.path.dirname(temp_path)
        config_path = os.path.join(file_dir, "config.json")

        try:
            # Create an invalid JSON file
            with open(config_path, "w") as f:
                f.write("{ invalid json }")

            # Should raise ValueError for invalid JSON
            with pytest.raises(ValueError, match="Failed to parse JSON"):
                dc_config_json_from_file(temp_path)
        finally:
            os.remove(temp_path)
            if os.path.exists(config_path):
                os.remove(config_path)


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


class TestRequirementsFile:
    def test_scan_file_for_imports(self):
        """Test scanning a file for external package imports."""
        content = textwrap.dedent(
            """
            import pandas as pd
            import numpy as np
            from sklearn.linear_model import LinearRegression
            import os  # Standard library
            import sys  # Standard library
            from datacustomcode.client import Client  # Internal package
            """
        )
        temp_path = create_test_script(content)
        try:
            imports = scan_file_for_imports(temp_path)
            assert "pandas" in imports
            assert "numpy" in imports
            assert "sklearn" in imports
            assert "os" not in imports  # Standard library
            assert "sys" not in imports  # Standard library
            assert "datacustomcode" not in imports  # Internal package
        finally:
            os.unlink(temp_path)

    def test_write_requirements_file_new(self):
        """Test writing a new requirements.txt file."""
        # Create a temporary directory structure
        temp_dir = tempfile.mkdtemp()
        script_dir = os.path.join(temp_dir, "script_dir")
        os.makedirs(script_dir)

        content = textwrap.dedent(
            """
            import pandas as pd
            import numpy as np
            """
        )
        temp_path = os.path.join(script_dir, "test_script.py")
        with open(temp_path, "w") as f:
            f.write(content)

        requirements_path = None
        try:
            requirements_path = write_requirements_file(temp_path)
            assert os.path.exists(requirements_path)
            assert (
                os.path.dirname(requirements_path) == temp_dir
            )  # Should be in parent directory

            with open(requirements_path, "r") as f:
                requirements = {line.strip() for line in f}

            assert "pandas" in requirements
            assert "numpy" in requirements
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            if requirements_path and os.path.exists(requirements_path):
                os.unlink(requirements_path)
            os.rmdir(script_dir)
            os.rmdir(temp_dir)

    def test_write_requirements_file_merge(self):
        """Test merging with existing requirements.txt file."""
        # Create a temporary directory structure
        temp_dir = tempfile.mkdtemp()
        script_dir = os.path.join(temp_dir, "script_dir")
        os.makedirs(script_dir)

        # Create existing requirements.txt in parent directory
        existing_requirements = os.path.join(temp_dir, "requirements.txt")
        with open(existing_requirements, "w") as f:
            f.write("pandas\nnumpy\n")

        # Create a new Python file with additional imports
        content = textwrap.dedent(
            """
            import pandas as pd
            import numpy as np
            import scipy
            import matplotlib
            """
        )
        temp_path = os.path.join(script_dir, "test_script.py")
        with open(temp_path, "w") as f:
            f.write(content)

        requirements_path = None
        try:
            requirements_path = write_requirements_file(temp_path)
            assert os.path.exists(requirements_path)
            assert (
                os.path.dirname(requirements_path) == temp_dir
            )  # Should be in parent directory

            with open(requirements_path, "r") as f:
                requirements = {line.strip() for line in f}

            # Check that both existing and new requirements are present
            assert "pandas" in requirements
            assert "numpy" in requirements
            assert "scipy" in requirements
            assert "matplotlib" in requirements
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            if requirements_path and os.path.exists(requirements_path):
                os.unlink(requirements_path)
            if os.path.exists(existing_requirements):
                os.unlink(existing_requirements)
            os.rmdir(script_dir)
            os.rmdir(temp_dir)

    def test_standard_library_exclusion(self):
        """Test that standard library imports are excluded."""
        content = textwrap.dedent(
            """
            import os
            import sys
            import json
            import datetime
            import pandas as pd
            """
        )
        temp_path = create_test_script(content)
        try:
            imports = scan_file_for_imports(temp_path)
            assert "pandas" in imports
            assert "os" not in imports
            assert "sys" not in imports
            assert "json" not in imports
            assert "datetime" not in imports
        finally:
            os.unlink(temp_path)

    def test_excluded_packages(self):
        """Test that excluded packages are not included in requirements."""
        content = textwrap.dedent(
            """
            import datacustomcode
            import pyspark
            import pandas as pd
            """
        )
        temp_path = create_test_script(content)
        try:
            imports = scan_file_for_imports(temp_path)
            assert "pandas" in imports
            assert "datacustomcode" not in imports
            assert "pyspark" not in imports
        finally:
            os.unlink(temp_path)
