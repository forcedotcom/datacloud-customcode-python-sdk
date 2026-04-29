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
import shutil

from loguru import logger

script_template_dir = os.path.join(os.path.dirname(__file__), "templates", "script")
function_template_dir = os.path.join(os.path.dirname(__file__), "templates", "function")


def copy_script_template(target_dir: str) -> None:
    """Copy the template to the target directory."""
    os.makedirs(target_dir, exist_ok=True)

    for item in os.listdir(script_template_dir):
        source = os.path.join(script_template_dir, item)
        destination = os.path.join(target_dir, item)

        if os.path.isdir(source):
            logger.debug(f"Copying directory {source} to {destination}...")
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            logger.debug(f"Copying file {source} to {destination}...")
            shutil.copy2(source, destination)


MAPPED_FOLDER = {"SearchIndexChunking": "chunking"}


def copy_function_template(target_dir: str, use_in_feature: str) -> None:
    os.makedirs(target_dir, exist_ok=True)

    if use_in_feature and use_in_feature in MAPPED_FOLDER:
        feature_function_template_dir = os.path.join(
            function_template_dir, MAPPED_FOLDER[use_in_feature]
        )
    else:
        feature_function_template_dir = function_template_dir

    for item in os.listdir(feature_function_template_dir):
        source = os.path.join(feature_function_template_dir, item)
        destination = os.path.join(target_dir, item)

        if os.path.isdir(source):
            logger.debug(f"Copying directory {source} to {destination}...")
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            logger.debug(f"Copying file {source} to {destination}...")
            shutil.copy2(source, destination)


