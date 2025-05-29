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
from pyspark.sql import SparkSession

from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode
from datacustomcode.io.reader.query_api import QueryAPIDataCloudReader


class PrintDataCloudWriter(BaseDataCloudWriter):
    CONFIG_NAME = "PrintDataCloudWriter"

    def validate_dataframe_columns_against_dlo(
        self, dataframe: PySparkDataFrame, dlo_name: str, reader: QueryAPIDataCloudReader
    ) -> None:
        """
        Validates that all columns in the given dataframe exist in the DLO schema.

        Args:
            dataframe (PySparkDataFrame): The DataFrame to validate.
            dlo_name (str): The name of the DLO to check against.
            reader (QueryAPIDataCloudReader): The reader to use for schema retrieval.

        Raises:
            ValueError: If any columns in the dataframe are not present in the DLO schema.
        """
        # Get DLO schema (no data, just schema)
        dlo_df = reader.read_dlo(dlo_name, row_limit=0)
        dlo_columns = set(dlo_df.columns)
        df_columns = set(dataframe.columns)

        # Find columns in dataframe not present in DLO
        extra_columns = df_columns - dlo_columns
        if extra_columns:
            raise ValueError(
                f"The following columns are not present in the DLO '{dlo_name}': {sorted(extra_columns)}.\n"
                "To fix this error, you can either:\n"
                "  - Drop these columns from your DataFrame before writing, e.g.,\n"
                "      dataframe = dataframe.drop({cols})\n"
                "  - Or, add these columns to the DLO schema in Data Cloud."
                .format(cols=sorted(extra_columns))
            )


    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        # Create SparkSession if not already created
        spark = SparkSession.builder.appName("YourAppName").getOrCreate()

        # Instantiate the reader
        reader = QueryAPIDataCloudReader(spark)

        # Validate columns before proceeding
        self.validate_dataframe_columns_against_dlo(dataframe, name, reader)

        dataframe.show()

    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode
    ) -> None:
        #The way its validating for DLO and dataframes columns, 
        # its not going to work for DMO because DMO may not exists, so just show the dataframe.

        dataframe.show()
