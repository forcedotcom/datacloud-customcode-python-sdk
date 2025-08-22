# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

import pytest

from datacustomcode.file.reader.default import (
    DefaultFileReader,
    FileAccessError,
    FileNotFoundError,
    FileReaderError,
)


class TestDefaultFileReader:
    """Test cases for the DefaultFileReader class."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        reader = DefaultFileReader()
        assert reader.code_package == "payload"
        assert reader.file_folder == "files"
        assert reader.config_file == "config.json"

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        reader = DefaultFileReader(
            code_package="custom_package",
            file_folder="custom_files",
            config_file="custom_config.json",
        )
        assert reader.code_package == "custom_package"
        assert reader.file_folder == "custom_files"
        assert reader.config_file == "custom_config.json"

    def test_file_open_with_empty_filename(self):
        """Test that read_file raises ValueError for empty filename."""
        reader = DefaultFileReader()
        with pytest.raises(ValueError, match="file_name cannot be empty"):
            reader.read_file("")

    def test_file_open_with_none_filename(self):
        """Test that read_file raises ValueError for None filename."""
        reader = DefaultFileReader()
        with pytest.raises(ValueError, match="file_name cannot be empty"):
            reader.read_file(None)

    @patch("datacustomcode.file.reader.default.DefaultFileReader._resolve_file_path")
    @patch("datacustomcode.file.reader.default.DefaultFileReader._open_file")
    def test_file_open_success(self, mock_open_file, mock_resolve_path):
        """Test successful file opening."""
        mock_path = Path("/test/path/file.txt")
        mock_file_handle = Mock()

        mock_resolve_path.return_value = mock_path
        mock_open_file.return_value = mock_file_handle

        reader = DefaultFileReader()
        result = reader.read_file("test.txt")

        assert result == mock_file_handle
        mock_resolve_path.assert_called_once_with("test.txt")
        mock_open_file.assert_called_once_with(mock_path)

    @patch("datacustomcode.file.reader.default.DefaultFileReader._resolve_file_path")
    def test_file_open_file_not_found(self, mock_resolve_path):
        """Test read_file when file is not found."""
        mock_resolve_path.return_value = None

        reader = DefaultFileReader()
        with pytest.raises(FileNotFoundError, match="File 'test.txt' not found"):
            reader.read_file("test.txt")

    @patch("datacustomcode.file.reader.default.DefaultFileReader._resolve_file_path")
    @patch("datacustomcode.file.reader.default.DefaultFileReader._open_file")
    def test_file_open_access_error(self, mock_open_file, mock_resolve_path):
        """Test read_file when there's an access error."""
        mock_path = Path("/test/path/file.txt")
        mock_resolve_path.return_value = mock_path
        mock_open_file.side_effect = PermissionError("Permission denied")

        reader = DefaultFileReader()
        with pytest.raises(FileAccessError, match="Error opening file"):
            reader.read_file("test.txt")

    def test_code_package_exists_true(self):
        """Test _code_package_exists when directory exists."""
        with patch("os.path.exists", return_value=True):
            reader = DefaultFileReader()
            assert reader._code_package_exists() is True

    def test_code_package_exists_false(self):
        """Test _code_package_exists when directory doesn't exist."""
        with patch("os.path.exists", return_value=False):
            reader = DefaultFileReader()
            assert reader._code_package_exists() is False

    def test_get_code_package_file_path(self):
        """Test _get_code_package_file_path."""
        reader = DefaultFileReader()
        result = reader._get_code_package_file_path("test.txt")
        expected = Path.cwd().joinpath("payload", "files", "test.txt")
        assert result == expected

    def test_get_config_based_file_path(self):
        """Test _get_config_based_file_path."""
        reader = DefaultFileReader()
        config_path = Path("/test/config.json")
        result = reader._get_config_based_file_path("test.txt", config_path)
        expected = Path("/test/files/test.txt")
        assert result == expected

    def test_find_file_in_tree_found(self):
        """Test _find_file_in_tree when file is found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_file = temp_path / "test.txt"
            test_file.write_text("test content")

            reader = DefaultFileReader()
            result = reader._find_file_in_tree("test.txt", temp_path)

            assert result == test_file

    def test_find_file_in_tree_not_found(self):
        """Test _find_file_in_tree when file is not found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            reader = DefaultFileReader()
            result = reader._find_file_in_tree("nonexistent.txt", temp_path)

            assert result is None

    def test_open_file(self):
        """Test _open_file method."""
        reader = DefaultFileReader()

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            temp_file.write("test content")
            temp_file_path = Path(temp_file.name)

        try:
            with reader._open_file(temp_file_path) as f:
                content = f.read()
                assert content == "test content"
        finally:
            os.unlink(temp_file_path)

    def test_get_search_locations_with_code_package(self):
        """Test get_search_locations when code package exists."""
        with patch(
            "datacustomcode.file.reader.default.DefaultFileReader._code_package_exists",
            return_value=True,
        ):
            with patch(
                "datacustomcode.file.reader.default.DefaultFileReader._find_config_file",
                return_value=None,
            ):
                reader = DefaultFileReader()
                locations = reader.get_search_locations()

                assert len(locations) == 1
                expected = Path.cwd().joinpath("payload", "files")
                assert locations[0] == expected

    def test_get_search_locations_with_config(self):
        """Test get_search_locations when config file exists."""
        with patch(
            "datacustomcode.file.reader.default.DefaultFileReader._code_package_exists",
            return_value=False,
        ):
            with patch(
                "datacustomcode.file.reader.default.DefaultFileReader._find_config_file",
                return_value=Path("/test/config.json"),
            ):
                reader = DefaultFileReader()
                locations = reader.get_search_locations()

                assert len(locations) == 1
                expected = Path("/test/files")
                assert locations[0] == expected

    def test_get_search_locations_both(self):
        """Test get_search_locations when both locations exist."""
        with patch(
            "datacustomcode.file.reader.default.DefaultFileReader._code_package_exists",
            return_value=True,
        ):
            with patch(
                "datacustomcode.file.reader.default.DefaultFileReader._find_config_file",
                return_value=Path("/test/config.json"),
            ):
                reader = DefaultFileReader()
                locations = reader.get_search_locations()

                assert len(locations) == 2
                expected_code_package = Path.cwd().joinpath("payload", "files")
                expected_config = Path("/test/files")
                assert locations[0] == expected_code_package
                assert locations[1] == expected_config

    def test_resolve_file_path_returns_filename_when_not_found(self):
        """Test _resolve_file_path returns Path(file_name)
        when file not found in any location."""
        reader = DefaultFileReader()

        # Mock both code package and config file to not exist or not contain the file
        with (
            patch.object(reader, "_code_package_exists", return_value=False),
            patch.object(reader, "_find_config_file", return_value=None),
        ):

            result = reader._resolve_file_path("nonexistent.txt")

            # Should return the filename as a Path object
            assert result == Path("nonexistent.txt")
            assert isinstance(result, Path)

    def test_file_path_returns_filename_when_code_package_exists_file_not_found(
        self,
    ):
        """Test _resolve_file_path returns Path(file_name)
        when code package exists but file not found."""
        reader = DefaultFileReader()

        # Mock code package exists but file doesn't exist in it
        with (
            patch.object(reader, "_code_package_exists", return_value=True),
            patch.object(reader, "_get_code_package_file_path") as mock_get_path,
            patch.object(reader, "_find_config_file", return_value=None),
            patch("pathlib.Path.exists", return_value=False),
        ):

            mock_path = Path("/test/payload/files/nonexistent.txt")
            mock_get_path.return_value = mock_path

            result = reader._resolve_file_path("nonexistent.txt")

            # Should return the filename as a Path object
            assert result == Path("nonexistent.txt")
            assert isinstance(result, Path)

    def test_file_path_returns_filename_when_config_file_exists_file_not_found(
        self,
    ):
        """Test _resolve_file_path returns Path(file_name)
        when config file exists but file not found."""
        reader = DefaultFileReader()

        # Mock code package doesn't exist but config file exists
        with (
            patch.object(reader, "_code_package_exists", return_value=False),
            patch.object(reader, "_find_config_file") as mock_find_config,
            patch.object(reader, "_get_config_based_file_path") as mock_get_config_path,
            patch("pathlib.Path.exists", return_value=False),
        ):

            # Mock config file found
            mock_config_path = Path("/test/config.json")
            mock_find_config.return_value = mock_config_path

            # Mock the config-based path to not exist
            mock_path = Path("/test/files/nonexistent.txt")
            mock_get_config_path.return_value = mock_path

            result = reader._resolve_file_path("nonexistent.txt")

            # Should return the filename as a Path object
            assert result == Path("nonexistent.txt")
            assert isinstance(result, Path)


class TestFileReaderIntegration:
    """Integration tests for the file reader."""

    def test_full_file_resolution_flow(self):
        """Test the complete file resolution and opening flow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a mock payload/files structure
            payload_dir = temp_path / "payload"
            files_dir = payload_dir / "files"
            files_dir.mkdir(parents=True)

            test_file = files_dir / "test.txt"
            test_file.write_text("test content")

            # Change to temp directory and test
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_path)

                reader = DefaultFileReader()
                with reader.read_file("test.txt") as f:
                    content = f.read()
                    assert content == "test content"
            finally:
                os.chdir(original_cwd)

    def test_fallback_to_config_based_location(self):
        """Test fallback from code package to config-based location."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create config.json but no payload directory
            config_file = temp_path / "config.json"
            config_file.write_text("{}")

            files_dir = temp_path / "files"
            files_dir.mkdir()

            test_file = files_dir / "test.txt"
            test_file.write_text("test content")

            # Change to temp directory and test
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_path)

                reader = DefaultFileReader()
                with reader.read_file("test.txt") as f:
                    content = f.read()
                    assert content == "test content"
            finally:
                os.chdir(original_cwd)


class TestFileReaderErrorHandling:
    """Test error handling scenarios."""

    def test_file_reader_error_inheritance(self):
        """Test that FileReaderError is the base exception."""
        assert issubclass(FileNotFoundError, FileReaderError)
        assert issubclass(FileAccessError, FileReaderError)

    def test_file_not_found_error_message(self):
        """Test FileNotFoundError message formatting."""
        error = FileNotFoundError("test.txt")
        assert "test.txt" in str(error)

    def test_file_access_error_message(self):
        """Test FileAccessError message formatting."""
        error = FileAccessError("test.txt", "Permission denied")
        assert "test.txt" in str(error)
        assert "Permission denied" in str(error)
