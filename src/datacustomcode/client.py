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

from enum import Enum
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
)

from pyspark.sql import SparkSession

from datacustomcode.config import SparkConfig, config
from datacustomcode.io.reader.base import BaseDataCloudReader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame

    from datacustomcode.io.reader.base import BaseDataCloudReader
    from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode


def _setup_spark(spark_config: SparkConfig) -> SparkSession:
    """Setup Spark session from config."""
    builder = SparkSession.builder
    if spark_config.master is not None:
        builder = builder.master(spark_config.master)

    builder = builder.appName(spark_config.app_name)
    for key, value in spark_config.options.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


class DataCloudObjectType(Enum):
    DLO = "dlo"
    DMO = "dmo"


class DataCloudAccessLayerException(Exception):
    """Exception raised when mixing DMOs and DLOs is detected."""

    def __init__(
        self,
        data_layer_history: dict[DataCloudObjectType, set[str]],
        should_not_contain: DataCloudObjectType,
    ) -> None:
        self.data_layer_history = data_layer_history
        self.should_not_contain = should_not_contain

    def __str__(self) -> str:
        msg = (
            "Mixed use of DMOs and DLOs. "
            "You can only read from DMOs to write to DMOs "
            "and read from DLOs to write to DLOs. "
        )
        if self.should_not_contain is DataCloudObjectType.DLO:
            msg += (
                "You have read from the following DLOs: "
                f"{self.data_layer_history[DataCloudObjectType.DLO]} "
                f"and are attempting to write to DMO. "
            )
        else:
            msg += (
                "You have read from the following DMOs: "
                f"{self.data_layer_history[DataCloudObjectType.DMO]} "
                f"and are attempting to write to to a DLO. "
            )
        msg += "Restart to clear history."
        return msg


class Client:
    """Entrypoint for accessing DataCloud objects.

    This is the object used to access Data Cloud DLOs and DMOs. Accessing DLOs/DMOs
    are tracked and will throw an exception if they are mixed. In other words, you
    can read from DLOs and write to DLOs, read from DMOs and write to DMOs, but you
    cannot read from DLOs and write to DMOs or read from DMOs and write to DLOs.
    Furthermore you cannot mix during merging tables. This class is a singleton to
    prevent accidental mixing of DLOs and DMOs.

    You can provide custom readers and writers to the client for advanced use
    cases, but this is not recommended for testing as they may result in unexpected
    behavior once deployed to Data Cloud. By default, the client intercepts all
    read/write operations and mocks access to Data Cloud. For example, during
    writing, we print to the console instead of writing to Data Cloud.

    Args:
        reader: A custom reader to use for reading Data Cloud objects.
        writer: A custom writer to use for writing Data Cloud objects.

    Example:
    >>> client = Client()
    >>> dlo = client.read_dlo("my_dlo")
    >>> client.write_to_dmo("my_dmo", dlo)
    """

    _instance: ClassVar[Optional[Client]] = None
    _reader: BaseDataCloudReader
    _writer: BaseDataCloudWriter
    _data_layer_history: dict[DataCloudObjectType, set[str]]

    def __new__(
        cls,
        reader: Optional[BaseDataCloudReader] = None,
        writer: Optional[BaseDataCloudWriter] = None,
    ) -> Client:
        if cls._instance is None:
            cls._instance = super().__new__(cls)

            # Initialize Readers and Writers from config
            # and/or provided reader and writer
            if reader is None or writer is None:
                # We need a spark because we will initialize readers and writers
                if config.spark_config is None:
                    raise ValueError(
                        "Spark config is required when reader/writer is not provided"
                    )
                spark = _setup_spark(config.spark_config)

            if config.reader_config is None and reader is None:
                raise ValueError(
                    "Reader config is required when reader is not provided"
                )
            elif reader is None or (
                config.reader_config is not None and config.reader_config.force
            ):
                reader_init = config.reader_config.to_object(spark)  # type: ignore
            else:
                reader_init = reader
            if config.writer_config is None and writer is None:
                raise ValueError(
                    "Writer config is required when writer is not provided"
                )
            elif writer is None or (
                config.writer_config is not None and config.writer_config.force
            ):
                writer_init = config.writer_config.to_object(spark)  # type: ignore
            else:
                writer_init = writer
            cls._instance._reader = reader_init
            cls._instance._writer = writer_init
            cls._instance._data_layer_history = {
                DataCloudObjectType.DLO: set(),
                DataCloudObjectType.DMO: set(),
            }
        elif (reader is not None or writer is not None) and cls._instance is not None:
            raise ValueError("Cannot set reader or writer after client is initialized")
        return cls._instance

    def read_dlo(self, name: str) -> PySparkDataFrame:
        """Read a DLO from Data Cloud.

        Args:
            name: The name of the DLO to read.

        Returns:
            A PySpark DataFrame containing the DLO data.
        """
        self._record_dlo_access(name)
        return self._reader.read_dlo(name)

    def read_dmo(self, name: str) -> PySparkDataFrame:
        """Read a DMO from Data Cloud.

        Args:
            name: The name of the DMO to read.

        Returns:
            A PySpark DataFrame containing the DMO data.
        """
        self._record_dmo_access(name)
        return self._reader.read_dmo(name)

    def write_to_dlo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode, **kwargs
    ) -> None:
        """Write a PySpark DataFrame to a DLO in Data Cloud.

        Args:
            name: The name of the DLO to write to.
            dataframe: The PySpark DataFrame to write.
            write_mode: The write mode to use for writing to the DLO.
        """
        self._validate_data_layer_history_does_not_contain(DataCloudObjectType.DMO)
        return self._writer.write_to_dlo(name, dataframe, write_mode, **kwargs)

    def write_to_dmo(
        self, name: str, dataframe: PySparkDataFrame, write_mode: WriteMode, **kwargs
    ) -> None:
        """Write a PySpark DataFrame to a DMO in Data Cloud.

        Args:
            name: The name of the DMO to write to.
            dataframe: The PySpark DataFrame to write.
            write_mode: The write mode to use for writing to the DMO.
        """
        self._validate_data_layer_history_does_not_contain(DataCloudObjectType.DLO)
        return self._writer.write_to_dmo(name, dataframe, write_mode, **kwargs)

    def _validate_data_layer_history_does_not_contain(
        self, data_cloud_object_type: DataCloudObjectType
    ) -> None:
        if len(self._data_layer_history[data_cloud_object_type]) > 0:
            raise DataCloudAccessLayerException(
                self._data_layer_history, data_cloud_object_type
            )

    def _record_dlo_access(self, name: str) -> None:
        self._data_layer_history[DataCloudObjectType.DLO].add(name)

    def _record_dmo_access(self, name: str) -> None:
        self._data_layer_history[DataCloudObjectType.DMO].add(name)
