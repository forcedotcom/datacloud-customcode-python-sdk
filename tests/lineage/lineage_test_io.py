"""Reader/writer for the generate tests, importable by the lineage worker process.

The worker re-imports this module (it is on ``sys.path`` via ``PYTHONPATH``), so
the writer reports validations through a file rather than shared memory.
"""

from __future__ import annotations

import json
import os

import pandas as pd

from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter

VALIDATED_FILE_ENV = "LINEAGE_TEST_VALIDATED_FILE"

TABLES = {
    "orders__dll": pd.DataFrame(
        {"id": [1, 2], "first": ["a", "b"], "last": ["x", "y"], "amount": [1.0, 2.0]}
    ),
    "customers__dlm": pd.DataFrame({"id": [1, 2], "region": ["us", "eu"]}),
}


class InMemoryReader(BaseDataCloudReader):
    CONFIG_NAME = "LineageTestInMemoryReader"

    def __init__(self, spark, dataspace=None):
        super().__init__(spark)

    def read_dlo(self, name, schema=None):
        return self.spark.createDataFrame(TABLES[name])

    read_dmo = read_dlo


class RefusingWriter(BaseDataCloudWriter):
    CONFIG_NAME = "LineageTestRefusingWriter"

    def __init__(self, spark, dataspace=None):
        super().__init__(spark)

    def validate_dataframe_columns_against_dlo(self, dataframe, dlo_name):
        with open(os.environ[VALIDATED_FILE_ENV], "a", encoding="utf-8") as f:
            f.write(json.dumps([dlo_name, list(dataframe.columns)]) + "\n")

    def write_to_dlo(self, name, dataframe, write_mode):
        raise AssertionError("lineage generation must never write")

    write_to_dmo = write_to_dlo
