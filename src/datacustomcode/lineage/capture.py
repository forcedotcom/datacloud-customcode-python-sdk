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
"""Readers and writers that record lineage instead of writing data.

While :func:`capture_lineage` is active, the global reader/writer config is
replaced (with ``force=True``) by :class:`LineageCaptureReader` and
:class:`LineageCaptureWriter`. They wrap the reader/writer the config had
already resolved, so the customer's reader choice and credentials still apply:
reads go through the delegate and are registered as lineage sources, writes are
validated through the delegate (when it supports it), traced, and never written.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

from datacustomcode.config import AccessLayerObjectConfig, config
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import (
    MERGE_RECORD_TYPE_COLUMN,
    BaseDataCloudWriter,
    WriteMode,
)
from datacustomcode.lineage.tracer import SourceRegistry, TraceResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType, StructType


@dataclass
class LineageResult:
    """Field-level lineage captured from one run of an entrypoint.

    Attributes:
        data_lineage: Target field to its sorted source fields.
        unresolved: Target field to the reason its lineage could not be traced.
        no_sources: Target fields computed from no source field (e.g. literals).
        targets: Objects written.
        sources: Objects read.
    """

    data_lineage: Dict[str, List[str]] = field(default_factory=dict)
    unresolved: Dict[str, str] = field(default_factory=dict)
    no_sources: List[str] = field(default_factory=list)
    targets: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)

    @property
    def pair_count(self) -> int:
        """Number of (target, source) field pairs in ``data_lineage``."""
        return sum(len(sources) for sources in self.data_lineage.values())

    def to_dict(self) -> Dict[str, Any]:
        """Deterministic JSON-ready form; ``dataLineage`` matches config.json."""
        return {
            "dataLineage": [
                {"targetField": target, "sourceFields": self.data_lineage[target]}
                for target in sorted(self.data_lineage)
            ],
            "unresolved": [
                {"targetField": target, "reason": self.unresolved[target]}
                for target in sorted(self.unresolved)
            ],
            "noSources": sorted(self.no_sources),
            "targets": sorted(self.targets),
            "sources": sorted(self.sources),
            "pairCount": self.pair_count,
        }


class LineageCollector:
    """Accumulates sources read and target fields written during a run."""

    def __init__(self) -> None:
        self.registry = SourceRegistry()
        self._targets: Dict[str, Dict[str, TraceResult]] = {}

    def record_read(self, object_name: str, dataframe: PySparkDataFrame) -> None:
        self.registry.register(dataframe, object_name)

    def record_write(self, object_name: str, dataframe: PySparkDataFrame) -> None:
        fields = self._targets.setdefault(object_name, {})
        for column, traced in self.registry.trace(dataframe).items():
            if column == MERGE_RECORD_TYPE_COLUMN:
                continue
            # Repeat writes to the same target: a field reads from the union of
            # every write, and stays unresolved if any write was unresolved
            previous = fields.get(column)
            if previous is None or (traced.unresolved and not previous.unresolved):
                fields[column] = traced
            elif not previous.unresolved:
                fields[column] = TraceResult(sources=previous.sources | traced.sources)

    def result(self) -> LineageResult:
        result = LineageResult(
            targets=list(self._targets), sources=list(self.registry.objects)
        )
        for object_name, fields in self._targets.items():
            for column, traced in fields.items():
                target = f"{object_name}.{column}"
                if traced.reason is not None:
                    result.unresolved[target] = traced.reason
                elif traced.sources:
                    result.data_lineage[target] = sorted(traced.sources)
                else:
                    result.no_sources.append(target)
        return result


@dataclass
class _CaptureSession:
    collector: LineageCollector
    reader_config: AccessLayerObjectConfig
    writer_config: AccessLayerObjectConfig


# The capture in progress, read by the capture classes when Client builds them
_active: Dict[str, Optional[_CaptureSession]] = {"session": None}


def _active_session() -> _CaptureSession:
    session = _active["session"]
    if session is None:
        raise RuntimeError(
            "Lineage capture readers and writers can only be created inside "
            "capture_lineage()"
        )
    return session


class _Delegating:
    """Falls back to the delegate for attributes the capture class lacks."""

    _delegate: Any

    def __getattr__(self, item: str) -> Any:
        if item == "_delegate":
            raise AttributeError(item)
        return getattr(self._delegate, item)


class LineageCaptureReader(_Delegating, BaseDataCloudReader):
    """Reads through the configured reader and registers lineage sources."""

    CONFIG_NAME = "LineageCaptureReader"

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)
        session = _active_session()
        self._collector = session.collector
        self._delegate = session.reader_config.to_object(spark)

    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        dataframe = (
            self._delegate.read_dlo(name)
            if schema is None
            else self._delegate.read_dlo(name, schema)
        )
        self._collector.record_read(name, dataframe)
        return dataframe  # type: ignore[no-any-return]

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        dataframe = (
            self._delegate.read_dmo(name)
            if schema is None
            else self._delegate.read_dmo(name, schema)
        )
        self._collector.record_read(name, dataframe)
        return dataframe  # type: ignore[no-any-return]


class LineageCaptureWriter(_Delegating, BaseDataCloudWriter):
    """Traces DataFrames handed to the writer; never writes or shows them."""

    CONFIG_NAME = "LineageCaptureWriter"

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)
        session = _active_session()
        self._collector = session.collector
        self._delegate = session.writer_config.to_object(spark)

    def _validate_dlo(self, name: str, dataframe: PySparkDataFrame) -> None:
        # Same column check `run` does, so lineage never names a missing field
        validate = getattr(
            self._delegate, "validate_dataframe_columns_against_dlo", None
        )
        if validate is not None:
            validate(dataframe, name)

    def write_to_dlo(
        self,
        name: str,
        dataframe: PySparkDataFrame,
        write_mode: WriteMode,
        **kwargs: Any,
    ) -> None:
        self.auto_write_to_dlo(name, dataframe)

    def write_to_dmo(
        self,
        name: str,
        dataframe: PySparkDataFrame,
        write_mode: WriteMode,
        **kwargs: Any,
    ) -> None:
        self.auto_write_to_dmo(name, dataframe)

    def auto_write_to_dlo(self, name: str, dataframe: PySparkDataFrame) -> None:
        self._validate_dlo(name, dataframe)
        self._collector.record_write(name, dataframe)

    def auto_write_to_dmo(self, name: str, dataframe: PySparkDataFrame) -> None:
        self._collector.record_write(name, dataframe)


@contextmanager
def capture_lineage(collector: LineageCollector) -> Iterator[LineageCollector]:
    """Route every ``Client`` created inside the block through lineage capture.

    The global reader/writer config is swapped for forced capture configs that
    wrap the currently configured reader/writer, and any existing ``Client``
    singleton is set aside so the entrypoint's ``Client()`` picks them up. Both
    are restored on exit.
    """
    from datacustomcode.client import Client, StreamingClient

    if config.reader_config is None or config.writer_config is None:
        raise ValueError("A reader and writer must be configured to capture lineage")
    if _active["session"] is not None:
        raise RuntimeError("Lineage capture is already active")

    saved_reader, saved_writer = config.reader_config, config.writer_config
    saved_clients = Client._instance, StreamingClient._instance
    _active["session"] = _CaptureSession(collector, saved_reader, saved_writer)
    config.reader_config = AccessLayerObjectConfig(
        type_config_name=LineageCaptureReader.CONFIG_NAME, force=True
    )
    config.writer_config = AccessLayerObjectConfig(
        type_config_name=LineageCaptureWriter.CONFIG_NAME, force=True
    )
    Client._instance = None
    StreamingClient._instance = None
    try:
        yield collector
    finally:
        config.reader_config, config.writer_config = saved_reader, saved_writer
        Client._instance, StreamingClient._instance = saved_clients
        _active["session"] = None
