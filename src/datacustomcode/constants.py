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

"""Constants used throughout the datacustomcode package."""

# File and directory names
ENTRYPOINT_FILE = "entrypoint.py"
CONFIG_FILE = "config.json"
PAYLOAD_DIR = "payload"
TESTS_DIR = "tests"
TEST_FILE = "test.json"
REQUIREMENTS_FILE = "requirements.txt"

# Default values
DEFAULT_PROFILE = "default"
DEFAULT_NETWORK = "default"
DEFAULT_CPU_SIZE = "CPU_2XL"

# Feature to template folder mapping
FEATURE_TEMPLATE_MAPPING = {
    "SearchIndexChunking": "chunking",
}

# Feature name to Connect API name mapping
USE_IN_FEATURE_MAPPING_FOR_CONNECT_API = {
    "SearchIndexChunking": "UnstructuredChunking",
}

# Pydantic request/response type names to feature names
REQUEST_TYPE_TO_FEATURE = {
    "SearchIndexChunkingV1Request": "SearchIndexChunking",
    "SearchIndexChunkingV1Response": "SearchIndexChunking",
}
