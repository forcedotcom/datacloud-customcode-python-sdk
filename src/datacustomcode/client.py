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
    Dict,
    Optional,
    Union,
)

from datacustomcode.config import config
from datacustomcode.file.path.default import DefaultFindFilePath
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.llm_gateway_config import spark_llm_gateway_config
from datacustomcode.spark.default import DefaultSparkSessionProvider

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import Column, DataFrame as PySparkDataFrame

    from datacustomcode.io.reader.base import BaseDataCloudReader
    from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode
    from datacustomcode.llm_gateway.spark_base import SparkLLMGateway
    from datacustomcode.spark.base import BaseSparkSessionProvider


def _build_spark_llm_gateway() -> "SparkLLMGateway":
    """Instantiate the SDK-configured :class:`SparkLLMGateway`.

    Raises:
        RuntimeError: If no ``spark_llm_gateway_config`` has been loaded.
    """
    cfg = spark_llm_gateway_config.spark_llm_gateway_config
    if cfg is None:
        raise RuntimeError(
            "spark_llm_gateway_config is not configured. Add a "
            "'spark_llm_gateway_config' section to config.yaml."
        )
    return cfg.to_object()


def llm_gateway_generate_text_col(
    template: str,
    values: Union[Dict[str, "Column"], "Column"],
    model_id: Optional[str] = None,
    max_tokens: Optional[int] = None,
) -> "Column":
    """Build a Spark Column that runs the LLM Gateway per row.

    Example:

        >>> df.withColumn(
        ...     "response",
        ...     llm_gateway_generate_text_col(
        ...         "In one sentence, greet {name} from {city}.",
        ...         {"name": col("name"), "city": col("city")},
        ...         model_id="sfdc_ai__DefaultGPT4Omni",
        ...         max_tokens=100,
        ...     ),
        ... )

    Args:
        template: The prompt template, with ``{field}`` placeholders matching
            keys in ``values``. Substitution uses ``str.format``.
        values: Either a mapping from placeholder name to Spark ``Column``, or
            a single ``Column`` whose value is already a struct.
        model_id: LLM model id. Defaults to ``sfdc_ai__DefaultGPT4Omni``.
        max_tokens: Maximum tokens to generate. Defaults to 200.

    Returns:
        A Spark ``Column`` that, when evaluated, produces the generated text.
    """
    gateway = Client()._get_spark_llm_gateway()
    return gateway.llm_gateway_generate_text_col(
        template, values, model_id=model_id, max_tokens=max_tokens
    )


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
        finder: Find a file path
        reader: A custom reader to use for reading Data Cloud objects.
        writer: A custom writer to use for writing Data Cloud objects.
        spark_llm_gateway: Optional custom :class:`SparkLLMGateway`. When
            omitted, the gateway is lazily resolved from
            ``spark_llm_gateway_config``.

    Example:
    >>> client = Client()
    >>> file_path = client.find_file_path("data.csv")
    >>> dlo = client.read_dlo("my_dlo")
    >>> client.write_to_dmo("my_dmo", dlo)
    >>> answer = client.llm_gateway_generate_text("Generate a greeting message")
    """

    _instance: ClassVar[Optional[Client]] = None
    _reader: BaseDataCloudReader
    _writer: BaseDataCloudWriter
    _file: DefaultFindFilePath
    _spark_llm_gateway: Optional[SparkLLMGateway]
    _data_layer_history: dict[DataCloudObjectType, set[str]]
    _code_type: str

    def __new__(
        cls,
        reader: Optional[BaseDataCloudReader] = None,
        writer: Optional[BaseDataCloudWriter] = None,
        spark_provider: Optional[BaseSparkSessionProvider] = None,
        spark_llm_gateway: Optional[SparkLLMGateway] = None,
        code_type: str = "script",
    ) -> Client:

        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._spark_llm_gateway = spark_llm_gateway
            # Initialize Readers and Writers from config
            # and/or provided reader and writer
            if reader is None or writer is None:
                # We need a spark because we will initialize readers and writers
                if config.spark_config is None:
                    raise ValueError(
                        "Spark config is required when reader/writer is not provided"
                    )

                provider: BaseSparkSessionProvider
                if spark_provider is not None:
                    provider = spark_provider
                elif config.spark_provider_config is not None:
                    provider = config.spark_provider_config.to_object()
                else:
                    provider = DefaultSparkSessionProvider()

                spark = provider.get_session(config.spark_config)

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
            cls._instance._file = DefaultFindFilePath()
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
        return self._reader.read_dlo(name)  # type: ignore[no-any-return]

    def read_dmo(self, name: str) -> PySparkDataFrame:
        """Read a DMO from Data Cloud.

        Args:
            name: The name of the DMO to read.

        Returns:
            A PySpark DataFrame containing the DMO data.
        """
        self._record_dmo_access(name)
        return self._reader.read_dmo(name)  # type: ignore[no-any-return]

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
        return self._writer.write_to_dlo(name, dataframe, write_mode, **kwargs)  # type: ignore[no-any-return]

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
        return self._writer.write_to_dmo(name, dataframe, write_mode, **kwargs)  # type: ignore[no-any-return]

    def find_file_path(self, file_name: str) -> Path:
        """Return a file path"""

        return self._file.find_file_path(file_name)  # type: ignore[no-any-return]

    def llm_gateway_generate_text(
        self,
        prompt: str,
        model_id: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        """Issue a one-shot LLM Gateway call. This is the scalar counterpart to
        :func:`llm_gateway_generate_text_col`: it runs **once**  — not per row.
        Use the column helper method instead when you want to fan a prompt out across
        every row of a DataFrame.

        Example:

            >>> response = Client().llm_gateway_generate_text(
            ...     "Generate a greeting message"
            ... )

        Args:
            prompt: The literal prompt to send. Plain text — no
                ``{field}`` substitution is performed on this string.
            model_id: LLM model id to target. Defaults to
                ``sfdc_ai__DefaultGPT4Omni`` when ``None``.
            max_tokens: Hard upper bound on the number of tokens the model
                may generate. Defaults to 200 when ``None``.

        Returns:
            The generated text as a plain Python ``str``; empty when the
            gateway response carries no generated text.
        """
        return self._get_spark_llm_gateway().llm_gateway_generate_text(
            prompt, model_id=model_id, max_tokens=max_tokens
        )

    def _get_spark_llm_gateway(self) -> SparkLLMGateway:
        if self._spark_llm_gateway is None:
            self._spark_llm_gateway = _build_spark_llm_gateway()
        return self._spark_llm_gateway

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
