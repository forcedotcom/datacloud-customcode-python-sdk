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
from __future__ import annotations

import os
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.file.path.default import (
    DefaultFindFilePath,
    FileNotFoundError,
    FileReaderError,
)


class TestDefaultFindFilePath:
    """Test cases for DefaultFindFilePath class."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        finder = DefaultFindFilePath()

        assert finder.code_package == "payload"
        assert finder.file_folder == "files"
        assert finder.config_file == "config.json"

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        finder = DefaultFindFilePath(
            code_package="custom_package",
            file_folder="custom_files",
            config_file="custom_config.json",
        )

        assert finder.code_package == "custom_package"
        assert finder.file_folder == "custom_files"
        assert finder.config_file == "custom_config.json"

    def test_resolve_library_path_files_subdir(self, tmp_path, monkeypatch):
        """$LIBRARY_PATH/<file_folder>/<name> resolves the BYOC layout."""
        files_dir = tmp_path / "files"
        files_dir.mkdir()
        target = files_dir / "data1.csv"
        target.write_text("hello")
        monkeypatch.setenv("LIBRARY_PATH", str(tmp_path))

        finder = DefaultFindFilePath()
        assert finder.find_file_path("data1.csv") == target

    def test_resolve_library_path_root_fallback(self, tmp_path, monkeypatch):
        """Fall back to $LIBRARY_PATH/<name> when files/<name> is missing."""
        target = tmp_path / "data1.csv"
        target.write_text("hello")
        monkeypatch.setenv("LIBRARY_PATH", str(tmp_path))

        finder = DefaultFindFilePath()
        assert finder.find_file_path("data1.csv") == target

    def test_resolve_library_path_subpath_under_files(self, tmp_path, monkeypatch):
        """Relative subpaths like 'file/data2.csv' resolve under $LIBRARY_PATH/files."""
        nested = tmp_path / "files" / "file"
        nested.mkdir(parents=True)
        target = nested / "data2.csv"
        target.write_text("hello")
        monkeypatch.setenv("LIBRARY_PATH", str(tmp_path))

        finder = DefaultFindFilePath()
        assert finder.find_file_path("file/data2.csv") == target

    def test_local_run_payload_files_default_layout(self, tmp_path, monkeypatch):
        """AC1: local-run resolves payload/files/<name> with no LIBRARY_PATH set.

        Mirrors ``datacustomcode run payload/entrypoint.py`` from a freshly
        ``init``ed package.
        """
        monkeypatch.delenv("LIBRARY_PATH", raising=False)
        package_dir = tmp_path / "my_package"
        files_dir = package_dir / "payload" / "files"
        files_dir.mkdir(parents=True)
        target = files_dir / "data1.csv"
        target.write_text("hello")
        monkeypatch.chdir(package_dir)

        finder = DefaultFindFilePath()
        result = finder.find_file_path("data1.csv")

        assert result.resolve() == target.resolve()

    def test_resolve_config_based_anchors_on_config_dir(self, tmp_path, monkeypatch):
        """config.json discovery anchors on the config's parent, not cwd.

        ``_find_config_file`` walks down from cwd via ``rglob``, so we put cwd
        at an ancestor of the package. The file lives only under
        ``<config_dir>/files/`` — a cwd-relative ``files/<name>`` would miss it.
        """
        monkeypatch.delenv("LIBRARY_PATH", raising=False)
        package_dir = tmp_path / "pkg"
        files_dir = package_dir / "files"
        files_dir.mkdir(parents=True)
        (package_dir / "config.json").write_text("{}")
        target = files_dir / "data1.csv"
        target.write_text("hello")

        monkeypatch.chdir(tmp_path)

        # Use a code_package that doesn't exist relative to tmp_path, so step 3
        # is skipped and resolution falls through to config.json discovery.
        finder = DefaultFindFilePath(code_package="nonexistent_pkg")
        result = finder.find_file_path("data1.csv")
        assert result.resolve() == target.resolve()

    def test_find_file_path_empty_filename(self):
        """Test find_file_path with empty filename raises ValueError."""
        finder = DefaultFindFilePath()

        with pytest.raises(ValueError, match="file_name cannot be empty"):
            finder.find_file_path("")

        with pytest.raises(ValueError, match="file_name cannot be empty"):
            finder.find_file_path(None)

    def test_find_file_path_file_not_found(self):
        """Test find_file_path when file doesn't exist raises FileNotFoundError."""
        finder = DefaultFindFilePath()

        with patch.object(finder, "_candidate_paths") as mock_candidates:
            mock_path = MagicMock()
            mock_path.exists.return_value = False
            mock_candidates.return_value = iter([mock_path])

            with pytest.raises(
                FileNotFoundError,
                match=r"File 'test\.txt' not found in any search location",
            ):
                finder.find_file_path("test.txt")

    def test_find_file_path_success(self):
        """Test find_file_path when file exists returns Path."""
        finder = DefaultFindFilePath()

        with patch.object(finder, "_candidate_paths") as mock_candidates:
            mock_path = MagicMock()
            mock_path.exists.return_value = True
            mock_candidates.return_value = iter([mock_path])

            result = finder.find_file_path("test.txt")

            assert result == mock_path
            mock_candidates.assert_called_once_with("test.txt")

    def test_find_file_path_env_var_set_file_exists(self):
        """find_file_path returns $LIBRARY_PATH/files/<name> when present."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            files_dir = Path(temp_dir) / "files"
            files_dir.mkdir()
            test_file = files_dir / "test.txt"
            test_file.write_text("test content")

            with patch.dict(os.environ, {finder.DEFAULT_ENV_VAR: str(temp_dir)}):
                result = finder.find_file_path("test.txt")

                assert result == test_file
                assert result.exists()

    def test_find_file_path_env_var_set_falls_through_to_code_package(self):
        """When $LIBRARY_PATH has no match, resolution falls through to code_package."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Set env var to a directory that doesn't contain the file
            with patch.dict(os.environ, {finder.DEFAULT_ENV_VAR: str(temp_dir)}):
                with patch.object(
                    finder, "_code_package_exists", return_value=True
                ) as mock_exists:
                    with patch.object(
                        finder, "_get_code_package_file_path"
                    ) as mock_get_path:
                        mock_path = MagicMock()
                        mock_path.exists.return_value = True
                        mock_get_path.return_value = mock_path

                        result = finder.find_file_path("test.txt")

                        assert result == mock_path
                        mock_exists.assert_called_once()
                        mock_get_path.assert_called_once_with("test.txt")

    def test_find_file_path_env_var_not_set_uses_code_package(self, monkeypatch):
        """With LIBRARY_PATH unset, code_package is the next candidate."""
        monkeypatch.delenv("LIBRARY_PATH", raising=False)
        finder = DefaultFindFilePath()

        with patch.object(
            finder, "_code_package_exists", return_value=True
        ) as mock_exists:
            with patch.object(finder, "_get_code_package_file_path") as mock_get_path:
                mock_path = MagicMock()
                mock_path.exists.return_value = True
                mock_get_path.return_value = mock_path

                result = finder.find_file_path("test.txt")

                assert result == mock_path
                mock_exists.assert_called_once()
                mock_get_path.assert_called_once_with("test.txt")

    def test_find_file_path_code_package_exists_falls_through_to_config(self):
        """When code_package candidate is missing, config.json discovery runs."""
        finder = DefaultFindFilePath()

        with patch.object(finder, "_code_package_exists", return_value=True):
            with patch.object(finder, "_get_code_package_file_path") as mock_get_path:
                with patch.object(finder, "_find_config_file") as mock_find_config:
                    with patch.object(
                        finder, "_get_config_based_file_path"
                    ) as mock_get_config_path:
                        # Code package file doesn't exist
                        mock_code_path = MagicMock()
                        mock_code_path.exists.return_value = False
                        mock_get_path.return_value = mock_code_path

                        # Config file exists and config-based file exists
                        mock_config_path = MagicMock()
                        mock_find_config.return_value = mock_config_path

                        mock_config_file_path = MagicMock()
                        mock_config_file_path.exists.return_value = True
                        mock_get_config_path.return_value = mock_config_file_path

                        result = finder.find_file_path("test.txt")

                        assert result == mock_config_file_path
                        mock_find_config.assert_called_once()
                        mock_get_config_path.assert_called_once_with(
                            "test.txt", mock_config_path
                        )

    def test_find_file_path_no_candidates_raises(self, monkeypatch):
        """When no candidate paths exist, find_file_path raises FileNotFoundError."""
        monkeypatch.delenv("LIBRARY_PATH", raising=False)
        finder = DefaultFindFilePath()

        with patch.object(finder, "_code_package_exists", return_value=False):
            with patch.object(finder, "_find_config_file", return_value=None):
                with pytest.raises(FileNotFoundError):
                    finder.find_file_path("test.txt")

    def test_find_file_path_error_lists_tried_locations(self, tmp_path, monkeypatch):
        """FileNotFoundError lists every candidate location that was tried."""
        env_dir = tmp_path / "env"
        env_dir.mkdir()
        monkeypatch.setenv("LIBRARY_PATH", str(env_dir))

        finder = DefaultFindFilePath()
        with pytest.raises(FileNotFoundError) as exc_info:
            finder.find_file_path("missing.txt")

        message = str(exc_info.value)
        assert "missing.txt" in message
        assert "Tried:" in message
        # LIBRARY_PATH candidates should appear
        assert str(env_dir / "files" / "missing.txt") in message
        assert str(env_dir / "missing.txt") in message

    def test_code_package_exists_true(self):
        """Test _code_package_exists returns True when directory exists."""
        finder = DefaultFindFilePath()

        with patch("os.path.exists", return_value=True):
            assert finder._code_package_exists() is True

    def test_code_package_exists_false(self):
        """Test _code_package_exists returns False when directory doesn't exist."""
        finder = DefaultFindFilePath()

        with patch("os.path.exists", return_value=False):
            assert finder._code_package_exists() is False

    def test_get_code_package_file_path(self):
        """Test _get_code_package_file_path constructs correct path."""
        finder = DefaultFindFilePath()

        result = finder._get_code_package_file_path("test.txt")

        expected = Path("payload/files/test.txt")
        assert result == expected

    def test_get_code_package_file_path_custom_values(self):
        """Test _get_code_package_file_path with custom values."""
        finder = DefaultFindFilePath(
            code_package="custom_package", file_folder="custom_files"
        )

        result = finder._get_code_package_file_path("test.txt")

        expected = Path("custom_package/custom_files/test.txt")
        assert result == expected

    def test_find_config_file_found(self):
        """Test _find_config_file when config file is found."""
        finder = DefaultFindFilePath()

        with patch.object(finder, "_find_file_in_tree") as mock_find:
            mock_path = MagicMock()
            mock_find.return_value = mock_path

            result = finder._find_config_file()

            assert result == mock_path
            mock_find.assert_called_once_with("config.json", Path.cwd())

    def test_find_config_file_not_found(self):
        """Test _find_config_file when config file is not found."""
        finder = DefaultFindFilePath()

        with patch.object(finder, "_find_file_in_tree", return_value=None):
            result = finder._find_config_file()

            assert result is None

    def test_get_config_based_file_path(self):
        """_get_config_based_file_path anchors on the discovered config dir."""
        finder = DefaultFindFilePath()
        config_path = Path("/some/path/config.json")

        result = finder._get_config_based_file_path("test.txt", config_path)

        expected = Path("/some/path/files/test.txt")
        assert result == expected

    def test_get_config_based_file_path_custom_folder(self):
        """_get_config_based_file_path uses custom file_folder under config dir."""
        finder = DefaultFindFilePath(file_folder="custom_files")
        config_path = Path("/some/path/config.json")

        result = finder._get_config_based_file_path("test.txt", config_path)

        expected = Path("/some/path/custom_files/test.txt")
        assert result == expected

    def test_find_file_in_tree_found(self):
        """Test _find_file_in_tree when file is found."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_file = temp_path / "test.txt"
            test_file.write_text("test content")

            result = finder._find_file_in_tree("test.txt", temp_path)

            assert result is not None
            assert result.name == "test.txt"

    def test_find_file_in_tree_not_found(self):
        """Test _find_file_in_tree when file is not found."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = finder._find_file_in_tree("nonexistent.txt", temp_path)

            assert result is None

    def test_find_file_in_tree_multiple_matches(self):
        """Test _find_file_in_tree when multiple files match, returns first one."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create multiple files with same name in different subdirectories
            (temp_path / "subdir1").mkdir()
            (temp_path / "subdir2").mkdir()

            file1 = temp_path / "subdir1" / "test.txt"
            file2 = temp_path / "subdir2" / "test.txt"

            file1.write_text("content1")
            file2.write_text("content2")

            result = finder._find_file_in_tree("test.txt", temp_path)

            assert result is not None
            assert result.name == "test.txt"
            # Should return one of the files (implementation returns first found)

    def test_integration_find_file_path_success(self):
        """Test integration: find_file_path with real file system."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test file
            test_file = Path(temp_dir) / "test.txt"
            test_file.write_text("test content")

            # Mock the code package to point to our temp directory
            finder.code_package = temp_dir
            finder.file_folder = ""

            result = finder.find_file_path("test.txt")

            assert result == test_file
            assert result.exists()

    def test_integration_find_file_path_not_found(self):
        """Test integration: find_file_path when file doesn't exist."""
        finder = DefaultFindFilePath()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Don't create any files
            finder.code_package = temp_dir
            finder.file_folder = ""

            with pytest.raises(FileNotFoundError):
                finder.find_file_path("nonexistent.txt")


class TestFileReaderError:
    """Test cases for FileReaderError exception classes."""

    def test_file_reader_error_inheritance(self):
        """Test FileReaderError inherits from Exception."""
        error = FileReaderError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_file_not_found_error_inheritance(self):
        """Test FileNotFoundError inherits from FileReaderError."""
        error = FileNotFoundError("file not found")
        assert isinstance(error, FileReaderError)
        assert isinstance(error, Exception)
        assert str(error) == "file not found"
