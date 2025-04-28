import os
import shutil
import sys
import tempfile
import textwrap

import pytest
import yaml

from datacustomcode.config import config
from datacustomcode.run import run_entrypoint


@pytest.fixture
def test_config_file():
    """Create a temporary test config file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as temp:
        test_config = {
            "spark_config": {
                "app_name": "TestApp",
                "master": "local[*]",
                "options": {"spark.executor.memory": "2g", "spark.driver.memory": "1g"},
                "force": True,
            },
            "reader_config": {
                "type_config_name": "test_reader",
                "options": {"path": "/path/to/input"},
                "force": False,
            },
            "writer_config": {
                "type_config_name": "test_writer",
                "options": {"path": "/path/to/output"},
                "force": False,
            },
        }
        yaml.dump(test_config, temp)
        temp_name = temp.name

    yield temp_name

    # Cleanup
    if os.path.exists(temp_name):
        os.unlink(temp_name)


@pytest.fixture
def test_entrypoint_file():
    """Create a temporary test entrypoint script."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as temp:
        entrypoint_content = textwrap.dedent(
            """
            # Test entrypoint
            import os
            import sys
            from datacustomcode.config import config

            # Store config values in a file to check later
            with open("test_entrypoint_output.txt", "w") as f:
                f.write(f"Config object ID: {id(config)}\\n")
                spark_app = (
                    config.spark_config.app_name if config.spark_config else None
                )
                f.write(f"Spark app name: {spark_app}\\n")
                reader_type = (
                    config.reader_config.type_config_name
                    if config.reader_config else None
                )
                f.write(f"Reader type: {reader_type}\\n")
                writer_type = (
                    config.writer_config.type_config_name
                    if config.writer_config else None
                )
                f.write(f"Writer type: {writer_type}\\n")
        """
        )
        temp.write(entrypoint_content.encode("utf-8"))
        temp_name = temp.name

    yield temp_name

    # Cleanup
    if os.path.exists(temp_name):
        os.unlink(temp_name)
    if os.path.exists("test_entrypoint_output.txt"):
        os.unlink("test_entrypoint_output.txt")


def test_run_entrypoint_preserves_config(test_config_file, test_entrypoint_file):
    """Test that run_entrypoint preserves config settings."""
    # Record initial config state
    initial_config_id = id(config)

    try:
        # Run entrypoint
        run_entrypoint(
            entrypoint=test_entrypoint_file,
            config_file=test_config_file,
            dependencies=[],
        )

        # Check that config was maintained
        assert id(config) == initial_config_id, "Config object identity changed"
        assert config.spark_config is not None, "Spark config is None"
        assert (
            config.spark_config.app_name == "TestApp"
        ), f"Expected 'TestApp', got {config.spark_config.app_name}"
        assert config.reader_config is not None, "Reader config is None"
        assert (
            config.reader_config.type_config_name == "test_reader"
        ), f"Expected 'test_reader', got {config.reader_config.type_config_name}"
        assert config.writer_config is not None, "Writer config is None"
        assert (
            config.writer_config.type_config_name == "test_writer"
        ), f"Expected 'test_writer', got {config.writer_config.type_config_name}"

        # Verify the entrypoint saw the same config
        with open("test_entrypoint_output.txt", "r") as f:
            lines = f.readlines()
            entrypoint_config_id = int(lines[0].split(":")[1].strip())
            entrypoint_app_name = lines[1].split(":")[1].strip()
            entrypoint_reader_type = lines[2].split(":")[1].strip()
            entrypoint_writer_type = lines[3].split(":")[1].strip()

        assert (
            entrypoint_config_id == initial_config_id
        ), "Entrypoint saw different config object"
        assert (
            entrypoint_app_name == "TestApp"
        ), f"Entrypoint saw app_name {entrypoint_app_name}, expected 'TestApp'"
        assert (
            entrypoint_reader_type == "test_reader"
        ), f"Reader_type {entrypoint_reader_type}, expected 'test_reader'"
        assert (
            entrypoint_writer_type == "test_writer"
        ), f"Writer_type {entrypoint_writer_type}, expected 'test_writer'"
    finally:
        # Clean up created files
        for file in ["test_entrypoint_output.txt"]:
            if os.path.exists(file):
                os.unlink(file)


def test_run_entrypoint_with_dependencies():
    """Test that run_entrypoint properly imports dependencies."""
    # Create a temp module to import
    module_dir = tempfile.mkdtemp()
    module_name = "test_dependency"
    module_path = os.path.join(module_dir, f"{module_name}.py")

    try:
        with open(module_path, "w") as f:
            f.write("TEST_CONSTANT = 'test_value'\n")

        sys.path.append(module_dir)

        # Create a temp entrypoint that uses the dependency
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as temp:
            entrypoint_content = textwrap.dedent(
                """
                # Test entrypoint with dependency
                import test_dependency

                # Store dependency value in a file to check later
                with open("dependency_output.txt", "w") as f:
                    f.write(f"Dependency value: {test_dependency.TEST_CONSTANT}\\n")
            """
            )
            temp.write(entrypoint_content.encode("utf-8"))
            entrypoint_file = temp.name

        # Create a minimal config file
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", delete=False, mode="w"
        ) as temp:
            temp.write("{}")  # Empty config
            config_file = temp.name

        # Run entrypoint with dependency
        run_entrypoint(
            entrypoint=entrypoint_file,
            config_file=config_file,
            dependencies=[module_name],
        )

        # Verify dependency was imported and used
        with open("dependency_output.txt", "r") as f:
            content = f.read()
            dependency_value = content.split(":")[1].strip()

        assert (
            dependency_value == "test_value"
        ), f"Expected 'test_value', got {dependency_value}"

    finally:
        # Cleanup safely
        if os.path.exists(entrypoint_file):
            os.unlink(entrypoint_file)
        if os.path.exists(config_file):
            os.unlink(config_file)
        if os.path.exists("dependency_output.txt"):
            os.unlink("dependency_output.txt")

        # Remove module from sys.modules
        if module_name in sys.modules:
            del sys.modules[module_name]

        # Safe directory removal
        if os.path.exists(module_dir):
            try:
                shutil.rmtree(module_dir)
            except Exception:
                pass

        # Remove the path we added
        if module_dir in sys.path:
            sys.path.remove(module_dir)
