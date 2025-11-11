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
from typing import Optional

from datacustomcode.file.base import BaseDataAccessLayer


class FileReaderError(Exception):
    """Base exception for file reader operations."""


class FileNotFoundError(FileReaderError):
    """Raised when a file cannot be found."""


class DefaultFindFilePath(BaseDataAccessLayer):
    """Base class for finding file path

    This class provides a framework for finding files from various locations
    with configurable search strategies and error handling.
    """

    # Default configuration values
    DEFAULT_ENV_VAR = "LIBRARY_PATH"
    DEFAULT_CODE_PACKAGE = "payload"
    DEFAULT_FILE_FOLDER = "files"
    DEFAULT_CONFIG_FILE = "config.json"

    def __init__(
        self,
        code_package: Optional[str] = None,
        file_folder: Optional[str] = None,
        config_file: Optional[str] = None,
    ):
        """Initialize the file reader with configuration.

        Args:
            code_package: The default code package directory to search
            file_folder: The folder containing files relative to the code package
            config_file: The configuration file to use for path resolution
        """
        self.code_package = code_package or self.DEFAULT_CODE_PACKAGE
        self.file_folder = file_folder or self.DEFAULT_FILE_FOLDER
        self.config_file = config_file or self.DEFAULT_CONFIG_FILE

    def find_file_path(self, file_name: str) -> Path:
        """Find a file path.

        Args:
            file_name: The name of the file to open

        Returns:
            A file path

        Raises:
            FileNotFoundError: If the file cannot be found
        """
        if not file_name:
            raise ValueError("file_name cannot be empty")

        file_path = self._resolve_file_path(file_name)

        if not file_path.exists():
            raise FileNotFoundError(
                f"File '{file_name}' not found in any search location"
            )

        return file_path

    def _resolve_file_path(self, file_name: str) -> Path:
        """Resolve the full path to a file.

        Args:
            file_name: The name of the file to resolve

        Returns:
            The full path to the file
        """
        # First check if environment variable is set
        env_path = os.getenv(self.DEFAULT_ENV_VAR)
        if env_path:
            file_path = Path(env_path) / file_name
            if file_path.exists():
                return file_path

        # First try the default code package location
        if self._code_package_exists():
            file_path = self._get_code_package_file_path(file_name)
            if file_path.exists():
                return file_path

        # Fall back to config.json-based location
        config_path = self._find_config_file()
        if config_path:
            file_path = self._get_config_based_file_path(file_name, config_path)
            if file_path.exists():
                return file_path

        # Return the file name as a Path if not found in any location
        return Path(file_name)

    def _code_package_exists(self) -> bool:
        """Check if the default code package directory exists.

        Returns:
            True if the code package directory exists
        """
        return os.path.exists(self.code_package)

    def _get_code_package_file_path(self, file_name: str) -> Path:
        """Get the file path relative to the code package.

        Args:
            file_name: The name of the file

        Returns:
            The full path to the file
        """
        relative_path = f"{self.code_package}/{self.file_folder}/{file_name}"
        return Path(relative_path)

    def _find_config_file(self) -> Optional[Path]:
        """Find the configuration file in the current directory tree.

        Returns:
            The path to the config file, or None if not found
        """
        return self._find_file_in_tree(self.config_file, Path.cwd())

    def _get_config_based_file_path(self, file_name: str, config_path: Path) -> Path:
        """Get the file path relative to the config file location.

        Args:
            file_name: The name of the file
            config_path: The path to the config file

        Returns:
            The full path to the file
        """
        relative_path = f"{self.file_folder}/{file_name}"
        return Path(relative_path)

    def _find_file_in_tree(self, filename: str, search_path: Path) -> Optional[Path]:
        """Find a file within a directory tree.

        Args:
            filename: The name of the file to find
            search_path: The root directory to search from

        Returns:
            The full path to the file, or None if not found
        """
        for file_path in search_path.rglob(filename):
            return file_path
        return None
