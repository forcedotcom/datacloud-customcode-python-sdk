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

from typing import Final

from datacustomcode.io.writer.base import MERGE_RECORD_TYPE_COLUMN, MergeRecordType

COMMIT_VERSION: Final = "_commit_version"
COMMIT_TIMESTAMP: Final = "_commit_timestamp"
MERGE_RECORD_TYPE: Final = MERGE_RECORD_TYPE_COLUMN

CDF_METADATA_COLUMNS: Final = (COMMIT_VERSION, COMMIT_TIMESTAMP, MERGE_RECORD_TYPE)

__all__ = [
    "COMMIT_VERSION",
    "COMMIT_TIMESTAMP",
    "MERGE_RECORD_TYPE",
    "CDF_METADATA_COLUMNS",
    "MergeRecordType",
]
