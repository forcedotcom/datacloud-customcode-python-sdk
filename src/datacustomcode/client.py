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
    Any,
    ClassVar,
    Dict,
    Optional,
    TypeVar,
    Union,
    cast,
)

from datacustomcode.config import config
from datacustomcode.einstein_predictions_config import spark_einstein_predictions_config
from datacustomcode.file.path.default import DefaultFindFilePath
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.llm_gateway_config import spark_llm_gateway_config
from datacustomcode.named_credential_config import spark_named_credential_config
from datacustomcode.spark.default import DefaultSparkSessionProvider

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import (
        Column,
        DataFrame as PySparkDataFrame,
        SparkSession,
    )
    from pyspark.sql.streaming import StreamingQuery

    from datacustomcode.einstein_predictions.spark_base import SparkEinsteinPredictions
    from datacustomcode.einstein_predictions.types import PredictionType
    from datacustomcode.io.reader.base import BaseDataCloudReader
    from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode
    from datacustomcode.llm_gateway.spark_base import SparkLLMGateway
    from datacustomcode.named_credential.spark_base import SparkNamedCredential
    from datacustomcode.named_credential.types.http_request import HTTPRequest
    from datacustomcode.named_credential.types.http_response import HTTPResponse
    from datacustomcode.spark.base import BaseSparkSessionProvider


def _streaming_source_name() -> str:
    """Return the streaming transform's read-source name.

    Resolved from ``config.streaming_source``, which ``run_entrypoint``
    populates from config.json's ``streamingSource`` field.

    Raises:
        RuntimeError: If no ``streaming_source`` has been configured (e.g. the
            transform's config.json has no ``streamingSource`` field).
    """
    source = config.streaming_source
    if not source:
        raise RuntimeError(
            "No streaming source configured. A streaming transform must declare "
            "its read source in config.json under 'streamingSource'."
        )
    return source


def _active_client() -> "_BaseClient":
    """Return the client backing the module-level Spark column helpers.

    Prefers an already-initialized singleton so a streaming job reuses its
    :class:`StreamingClient` (and a batch job its :class:`Client`) rather than
    forcing an unrelated client into existence. Falls back to building the
    batch :class:`Client` when neither has been created yet.
    """
    if Client._instance is not None:
        return Client._instance
    if StreamingClient._instance is not None:
        return StreamingClient._instance
    return Client()


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
) -> "Column":
    """Build a Spark Column that runs the LLM Gateway per row.

    The returned Column yields a struct ``{status, response, error_code,
    error_message}`` for each row. Use ``[...]`` (or ``getField``) to pick the
    field you want, e.g. ``llm_gateway_generate_text_col(...)["response"]``.
    Per-row failures populate ``status`` / ``error_code`` / ``error_message``
    so a single bad row does not abort the whole Spark job.

    Example:

        >>> result = llm_gateway_generate_text_col(
        ...     "In one sentence, greet {name} from {city}.",
        ...     {"name": col("name__c"), "city": col("homecity__c")},
        ...     model_id="sfdc_ai__DefaultGPT4Omni",
        ... )
        >>> df.withColumn("greeting__c", result["response"])
        >>> # …or keep the struct around and inspect failures:
        >>> df.withColumn("llm", result).select(
        ...     "llm.status", "llm.response", "llm.error_message"
        ... )

    Args:
        template: The prompt template, with ``{field}`` placeholders matching
            keys in ``values``. Substitution uses ``str.format``.
        values: Either a mapping from placeholder name to Spark ``Column``, or
            a single ``Column`` whose value is already a struct.
        model_id: LLM model id. Defaults to ``sfdc_ai__DefaultGPT4Omni``.

    Returns:
        A Spark ``Column`` of ``StructType`` with fields ``status``,
        ``response``, ``error_code``, and ``error_message`` (all nullable
        strings). On success, ``status == "SUCCESS"`` and ``response`` holds
        the generated text; on failure, ``status == "ERROR"`` and the
        ``error_*`` fields carry diagnostic detail.
    """
    gateway = _active_client()._get_spark_llm_gateway()
    return gateway.llm_gateway_generate_text_col(template, values, model_id=model_id)


def _build_spark_einstein_predictions() -> "SparkEinsteinPredictions":
    """Instantiate the SDK-configured :class:`SparkEinsteinPredictions`.

    Raises:
        RuntimeError: If no ``spark_einstein_predictions_config`` has been loaded.
    """
    cfg = spark_einstein_predictions_config.spark_einstein_predictions_config
    if cfg is None:
        raise RuntimeError(
            "spark_einstein_predictions_config is not configured. Add a "
            "'spark_einstein_predictions_config' section to config.yaml."
        )
    return cfg.to_object()


def _build_spark_named_credential() -> "SparkNamedCredential":
    """Instantiate the SDK-configured :class:`SparkNamedCredential`.

    Raises:
        RuntimeError: If no ``spark_named_credential_config`` has been loaded.
    """
    cfg = spark_named_credential_config.spark_named_credential_config
    if cfg is None:
        raise RuntimeError(
            "spark_named_credential_config is not configured. Add a "
            "'spark_named_credential_config' section to config.yaml."
        )
    return cfg.to_object()


def einstein_predict_col(
    model_api_name: str,
    prediction_type: "PredictionType",
    features: Dict[str, "Column"],
    settings: Optional[Dict[str, Any]] = None,
) -> "Column":
    """Build a Spark Column that runs an Einstein prediction per row.

    The returned Column yields a struct ``{status, response, error_code,
    error_message}`` for each row. Use ``[...]`` (or ``getField``) to pick the
    field you want, e.g. ``einstein_predict_col(...)["response"]``. ``response``
    holds the prediction response payload as a JSON string. Per-row failures
    populate ``status`` / ``error_code`` / ``error_message`` so a single bad row
    does not abort the whole Spark job.

    Example:

        >>> from datacustomcode.einstein_predictions.types import PredictionType
        >>> result = einstein_predict_col(
        ...     "my_regression_model",
        ...     PredictionType.REGRESSION,
        ...     {"square_feet": col("square_feet__c"), "beds": col("beds__c")},
        ... )
        >>> df.withColumn("prediction__c", result["response"])
        >>> # …or keep the struct around and inspect failures:
        >>> df.withColumn("pred", result).select(
        ...     "pred.status", "pred.response", "pred.error_message"
        ... )

    Args:
        model_api_name: API name of the Einstein model to invoke.
        prediction_type: The :class:`PredictionType` of the model.
        features: A mapping from model feature column name to a Spark ``Column``
            supplying that feature's per-row value.
        settings: Optional prediction settings forwarded to the model.

    Returns:
        A Spark ``Column`` of ``StructType`` with fields ``status``,
        ``response``, ``error_code``, and ``error_message`` (all nullable
        strings). On success, ``status == "SUCCESS"`` and ``response`` holds
        the JSON-serialized prediction payload; on failure, ``status ==
        "ERROR"`` and the ``error_*`` fields carry diagnostic detail.
    """
    predictions = _active_client()._get_spark_einstein_predictions()
    return predictions.einstein_predict_col(
        model_api_name, prediction_type, features, settings=settings
    )


def named_credential_request_col(
    request: "HTTPRequest",
    body: Optional["Column"] = None,
) -> "Column":
    """Build a Spark Column that makes one Named Credential callout per row.

    The endpoint, method, and headers are fixed for the call (taken from
    ``request``); only ``body`` varies per row. Use this instead of
    :meth:`Client.named_credential_request` when the callout runs across a
    DataFrame so each row is dispatched independently rather than one-shot on
    the driver.

    The returned Column yields a struct ``{status, response, error_code,
    error_message}`` for each row. ``response`` is itself a struct
    ``{status_code, body, headers}``. Use ``[...]`` to pick a field, e.g.
    ``named_credential_request_col(...)["response"]["status_code"]``. A transport
    failure sets ``status`` to ``ERROR`` and populates ``error_message`` (a non-2xx
    HTTP response is still ``SUCCESS`` with its code in ``response.status_code``),
    so a single bad row does not abort the whole Spark job.

    Args:
        request: The callout template — its symbolic reference, method, and
            headers are applied to every row.
        body: Optional per-row ``Column`` holding the request body as a
            string (or null for no body).

    Returns:
        A Spark ``Column`` of ``StructType`` with fields ``status``,
        ``response``, ``error_code``, and ``error_message``.
    """
    named_credential = Client()._get_spark_named_credential()
    return named_credential.request_col(request, body=body)


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


_ClientT = TypeVar("_ClientT", bound="_BaseClient")


class _BaseClient:
    """Shared machinery for the Data Cloud client singletons.

    Holds the wiring common to :class:`Client` (batch) and
    :class:`StreamingClient`

    This base class is not meant to be instantiated directly; use
    :class:`Client` or :class:`StreamingClient`.

    Args:
        reader: A custom reader to use for reading Data Cloud objects.
        writer: A custom writer to use for writing Data Cloud objects.
        spark_provider: Optional custom :class:`BaseSparkSessionProvider`.
        spark_llm_gateway: Optional custom :class:`SparkLLMGateway`.
        spark_einstein_predictions: Optional custom
            :class:`SparkEinsteinPredictions`.
        spark_named_credential: Optional custom :class:`SparkNamedCredential`.

    Example:
    >>> client = Client()
    >>> file_path = client.find_file_path("data.csv")
    >>> dlo = client.read_dlo("my_dlo")
    >>> client.write_to_dmo("my_dmo", dlo)
    >>> answer = client.llm_gateway_generate_text("Generate a greeting message")
    """

    # Each concrete subclass gets its own ``_instance`` slot: reads fall through
    # to this base default of ``None``, but ``cls._instance = ...`` in __new__
    # always writes to the subclass, so ``Client`` and ``StreamingClient`` never
    # share an instance.
    _instance: ClassVar[Optional[_BaseClient]] = None
    # Process-wide Spark session shared across BOTH client types. Unlike
    # ``_instance``, this is written via ``_BaseClient._shared_spark`` (never
    # ``cls._shared_spark``), so the slot lives on the base class and a
    # ``Client`` and a ``StreamingClient`` in the same process reuse one session
    # — and therefore one underlying connection — instead of opening two
    # containing differing state
    _shared_spark: ClassVar[Optional[SparkSession]] = None
    _reader: BaseDataCloudReader
    _writer: BaseDataCloudWriter
    _file: DefaultFindFilePath
    _spark_llm_gateway: Optional[SparkLLMGateway]
    _spark_einstein_predictions: Optional[SparkEinsteinPredictions]
    _spark_named_credential: Optional[SparkNamedCredential]
    _data_layer_history: dict[DataCloudObjectType, set[str]]
    _code_type: str

    def __new__(
        cls: type[_ClientT],
        reader: Optional[BaseDataCloudReader] = None,
        writer: Optional[BaseDataCloudWriter] = None,
        spark_provider: Optional[BaseSparkSessionProvider] = None,
        spark_llm_gateway: Optional[SparkLLMGateway] = None,
        spark_einstein_predictions: Optional[SparkEinsteinPredictions] = None,
        spark_named_credential: Optional[SparkNamedCredential] = None,
        code_type: str = "script",
    ) -> _ClientT:

        if cls._instance is None:
            instance = super().__new__(cls)
            instance._spark_llm_gateway = spark_llm_gateway
            instance._spark_einstein_predictions = spark_einstein_predictions
            instance._spark_named_credential = spark_named_credential
            # Initialize Readers and Writers from config
            # and/or provided reader and writer
            if reader is None or writer is None:
                # We need a spark because we will initialize readers and writers.
                # Reuse the process-wide session if one client already built it,
                # so a Client and a StreamingClient share a single connection.
                if _BaseClient._shared_spark is not None:
                    spark = _BaseClient._shared_spark
                else:
                    if config.spark_config is None:
                        raise ValueError(
                            "Spark config is required when reader/writer is not "
                            "provided"
                        )

                    provider: BaseSparkSessionProvider
                    if spark_provider is not None:
                        provider = spark_provider
                    elif config.spark_provider_config is not None:
                        provider = config.spark_provider_config.to_object()
                    else:
                        provider = DefaultSparkSessionProvider()

                    spark = provider.get_session(config.spark_config)
                    _BaseClient._shared_spark = spark

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

            instance._reader = reader_init
            instance._writer = writer_init
            instance._file = DefaultFindFilePath()
            instance._data_layer_history = {
                DataCloudObjectType.DLO: set(),
                DataCloudObjectType.DMO: set(),
            }
            cls._instance = instance
        elif reader is not None or writer is not None:
            raise ValueError("Cannot set reader or writer after client is initialized")
        return cast(_ClientT, cls._instance)

    def find_file_path(self, file_name: str) -> Path:
        """Resolve a bundled file shipped in the package to an absolute path.

        Resolution order (first existing path wins):

        1. ``$LIBRARY_PATH/<file_folder>/<file_name>`` then
           ``$LIBRARY_PATH/<file_name>`` — when the ``LIBRARY_PATH`` environment
           variable is set. The Data Cloud runtime sets this to the directory
           containing the extracted package.
        2. ``<code_package>/<file_folder>/<file_name>`` relative to the current
           working directory — the default ``payload/files/<file_name>`` layout
           used by ``datacustomcode run`` from a project root.
        3. ``<config_dir>/<file_folder>/<file_name>`` where ``<config_dir>`` is
           the directory containing the nearest ``config.json`` discoverable
           by walking the cwd subtree.

        ``LIBRARY_PATH`` must point to the directory that *contains*
        ``files/`` — i.e., the package root, the same directory that holds
        ``config.json`` and ``entrypoint.py``. See ``Bundled file resolution``
        in the ``readme.md`` for more details.

        Args:
            file_name: A file under the package's ``files/`` folder. Relative
                subpaths (e.g., ``"file/data2.csv"``) are supported.

        Returns:
            A ``pathlib.Path`` that exists.

        Raises:
            FileNotFoundError: If the file does not exist at any of the
                resolution-order locations. The message lists every candidate
                path that was tried.
        """
        return self._file.find_file_path(file_name)  # type: ignore[no-any-return]

    def llm_gateway_generate_text(
        self,
        prompt: str,
        model_id: Optional[str] = None,
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

        Returns:
            The generated text as a plain Python ``str``; empty when the
            gateway response carries no generated text.
        """
        return self._get_spark_llm_gateway().llm_gateway_generate_text(
            prompt, model_id=model_id
        )

    def _get_spark_llm_gateway(self) -> SparkLLMGateway:
        if self._spark_llm_gateway is None:
            self._spark_llm_gateway = _build_spark_llm_gateway()
        return self._spark_llm_gateway

    def einstein_predict(
        self,
        model_api_name: str,
        prediction_type: "PredictionType",
        features: Dict[str, Any],
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Issue a one-shot Einstein prediction. This is the scalar counterpart
        to :func:`einstein_predict_col`: it runs **once** — not per row. Use the
        column helper method instead when you want to fan a prediction out
        across every row of a DataFrame.

        Example:

            >>> from datacustomcode.einstein_predictions.types import PredictionType
            >>> response = Client().einstein_predict(
            ...     "my_regression_model",
            ...     PredictionType.REGRESSION,
            ...     {"square_feet": 1800, "beds": 3},
            ... )

        Args:
            model_api_name: API name of the Einstein model to invoke.
            prediction_type: The :class:`PredictionType` of the model.
            features: A mapping from model feature column name to a single
                scalar value (``str`` / ``float`` / ``bool``).
            settings: Optional prediction settings forwarded to the model.

        Returns:
            The prediction response payload as a plain Python ``dict``.

        Raises:
            EinsteinPredictionsCallError: If the prediction call fails.
        """
        return self._get_spark_einstein_predictions().einstein_predict(
            model_api_name, prediction_type, features, settings=settings
        )

    def _get_spark_einstein_predictions(self) -> SparkEinsteinPredictions:
        if self._spark_einstein_predictions is None:
            self._spark_einstein_predictions = _build_spark_einstein_predictions()
        return self._spark_einstein_predictions

    def named_credential_request(
        self,
        request: "HTTPRequest",
        body: Optional[str] = None,
    ) -> "HTTPResponse":
        """Issue a one-shot Named Credential external callout. This is the
        scalar counterpart to :func:`named_credential_request_col`: it runs
        **once** on the driver — not per row. Use the column helper method
        instead when you want to fan a callout out across every row of a
        DataFrame.

        Example:

            >>> from datacustomcode.named_credential.types.http_request_builder \\
            ...     import HTTPRequestBuilder
            >>> request = (
            ...     HTTPRequestBuilder().set_url("callout:NC/search").build()
            ... )
            >>> response = Client().named_credential_request(request)

        Args:
            request: The callout request
            body: Optional request body. Set the ``Content-Type`` header to
                match the format; the SDK does not assume or inject one.

        Returns:
            The external service's response.
        """
        return self._get_spark_named_credential().request(request, body=body)

    def _get_spark_named_credential(self) -> SparkNamedCredential:
        if self._spark_named_credential is None:
            self._spark_named_credential = _build_spark_named_credential()
        return self._spark_named_credential

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


class Client(_BaseClient):
    """Entrypoint for batch access to Data Cloud objects.

    This is the object used to read and write bounded snapshots of Data Cloud
    DLOs and DMOs.
    """

    _instance: ClassVar[Optional[Client]] = None

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


class StreamingClient(_BaseClient):
    """Entrypoint for streaming (``DELTA_SYNC``) access to Data Cloud objects.

    This is the streaming counterpart to :class:`Client`. Instead of reading and
    writing bounded snapshots, it reads a DLO/DMO change feed as a streaming
    DataFrame and writes the transformed stream back via a ``StreamingQuery``.
    """

    _instance: ClassVar[Optional[StreamingClient]] = None

    def read_dlo_deltas(self) -> PySparkDataFrame:
        """Read the streaming change feed (deltas) for a DLO from Data Cloud.

        For use in a streaming (``DELTA_SYNC``) BYOC transform. Returns a
        streaming DataFrame whose rows carry the change-feed metadata columns
        (``_record_type``, ``_commit_*``) alongside the source columns.

        Returns:
            A streaming PySpark DataFrame over the DLO change feed.
        """
        self._record_dlo_access(_streaming_source_name())
        return self._reader.read_dlo_deltas()  # type: ignore[no-any-return]

    def read_dmo_deltas(self) -> PySparkDataFrame:
        """Read the streaming change feed (deltas) for a DMO from Data Cloud.

        Returns:
            A streaming PySpark DataFrame over the DMO change feed.
        """
        self._record_dmo_access(_streaming_source_name())
        return self._reader.read_dmo_deltas()  # type: ignore[no-any-return]

    def write_dlo_deltas(
        self, name: str, dataframe: PySparkDataFrame, **kwargs
    ) -> StreamingQuery:
        """Write a streaming DataFrame of deltas to a DLO in Data Cloud.

        Starts a streaming query that writes each micro-batch to the
        target DLO and returns the  ``StreamingQuery`` handle; the caller
        typically calls ``query.awaitTermination()``.

        Args:
            name: The name of the DLO to write to.
            dataframe: The streaming PySpark DataFrame to write.

        Returns:
            The started ``StreamingQuery``.
        """
        self._validate_data_layer_history_does_not_contain(DataCloudObjectType.DMO)
        return self._writer.write_dlo_deltas(name, dataframe, **kwargs)  # type: ignore[no-any-return]
