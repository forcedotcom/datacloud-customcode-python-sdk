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

import io
import os

from abc import abstractmethod
from pathlib import Path
from datacustomcode.file.base import BaseDataAccessLayer

class BaseFileReader(BaseDataAccessLayer):
    def file_open(self, file_name: str) -> io.TextIOWrapper:
        default_code_pacakge = 'payload'
        file_folder = 'files' # hardcoded folder name

        file_path = None
        if os.path.exists(default_code_pacakge):
            relative = f"{default_code_pacakge}/{file_folder}/{file_name}"
            file_path = Path.cwd().joinpath(relative)
        else:
            # find config.json, files folder is right next to the config.json
            config_abs_path = Path(self.find_file_pathlib("config.json", Path.cwd()))
            relative = f"{file_folder}/{file_name}"
            file_path = config_abs_path.parent.joinpath(relative)
        
        file_handle = open(file_path, 'r')
        #print(f"chuy file type: {type(foo), file_path}")
        return file_handle

    def find_file_pathlib(self, filename, search_path):
        """
        Finds a file within a directory and its subdirectories using pathlib.
        Returns the full path of the first match found, or None if not found.
        """
        path_obj = Path(search_path)
        for file_path in path_obj.rglob(filename):
            return str(file_path)  # Convert Path object to string
        return None