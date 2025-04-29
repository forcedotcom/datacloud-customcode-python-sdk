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


from pyspark.sql import DataFrame as PySparkDataFrame

from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode

SUFFIX = ".csv"


class CSVDataCloudWriter(BaseDataCloudWriter):
    CONFIG_NAME = "CSVDataCloudWriter"

    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        # Only add the suffix if it's not already there
        if not name.lower().endswith(SUFFIX):
            name = f"{name}{SUFFIX}"
        dataframe.write.csv(name, mode=write_mode)

    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        # Only add the suffix if it's not already there
        if not name.lower().endswith(SUFFIX):
            name = f"{name}{SUFFIX}"
        dataframe.write.csv(name, mode=write_mode)
