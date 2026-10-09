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

While :func:`capture_lineage` is active, the reader and writer of every client
(already existing or created inside the block) are wrapped in place by
:class:`LineageCaptureReader` and :class:`LineageCaptureWriter`. The wrappers
delegate to the reader/writer the client actually resolved, whether from config
or passed to ``Client(reader=..., writer=...)``, so the customer's reader choice
and credentials still apply: reads go through the delegate and are registered as
lineage sources, writes are validated through the delegate (when it supports
it), traced, and never written.
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

from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import (
    MERGE_RECORD_TYPE_COLUMN,
    BaseDataCloudWriter,
    WriteMode,
)
from datacustomcode.lineage.tracer import SourceRegistry, TraceResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame
    from pyspark.sql.types import AtomicType, StructType

    from datacustomcode.client import _BaseClient


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


class _Delegating:
    """Falls back to the delegate for attributes the capture class lacks."""

    _delegate: Any

    def __getattr__(self, item: str) -> Any:
        if item == "_delegate":
            raise AttributeError(item)
        return getattr(self._delegate, item)


class LineageCaptureReader(_Delegating, BaseDataCloudReader):
    """Reads through a client's reader and registers lineage sources."""

    def __init__(
        self, delegate: BaseDataCloudReader, collector: LineageCollector
    ) -> None:
        super().__init__(getattr(delegate, "spark", None))  # type: ignore[arg-type]
        self._delegate = delegate
        self._collector = collector

    def read_dlo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        dataframe: PySparkDataFrame = (
            self._delegate.read_dlo(name)
            if schema is None
            else self._delegate.read_dlo(name, schema)
        )
        self._collector.record_read(name, dataframe)
        return dataframe

    def read_dmo(
        self,
        name: str,
        schema: Union[AtomicType, StructType, str, None] = None,
    ) -> PySparkDataFrame:
        dataframe: PySparkDataFrame = (
            self._delegate.read_dmo(name)
            if schema is None
            else self._delegate.read_dmo(name, schema)
        )
        self._collector.record_read(name, dataframe)
        return dataframe


class LineageCaptureWriter(_Delegating, BaseDataCloudWriter):
    """Traces DataFrames handed to the writer; never writes or shows them."""

    def __init__(
        self, delegate: BaseDataCloudWriter, collector: LineageCollector
    ) -> None:
        super().__init__(getattr(delegate, "spark", None))  # type: ignore[arg-type]
        self._delegate = delegate
        self._collector = collector

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


@dataclass
class _WrappedClient:
    client: _BaseClient
    reader: BaseDataCloudReader
    writer: BaseDataCloudWriter


class _CaptureSession:
    def __init__(self, collector: LineageCollector) -> None:
        self.collector = collector
        self._wrapped: Dict[int, _WrappedClient] = {}

    def wrap(self, client: _BaseClient) -> None:
        """Route a client's reads and writes through capture (idempotent)."""
        if id(client) in self._wrapped:
            return
        reader, writer = client._reader, client._writer
        self._wrapped[id(client)] = _WrappedClient(client, reader, writer)
        client._reader = LineageCaptureReader(reader, self.collector)
        client._writer = LineageCaptureWriter(writer, self.collector)

    def restore(self) -> None:
        for wrapped in self._wrapped.values():
            wrapped.client._reader = wrapped.reader
            wrapped.client._writer = wrapped.writer
        self._wrapped.clear()


# The capture in progress; guards against nested captures
_active: Dict[str, Optional[_CaptureSession]] = {"session": None}


@contextmanager
def capture_lineage(collector: LineageCollector) -> Iterator[LineageCollector]:
    """Capture lineage from every ``Client`` used inside the block.

    Clients that already exist (including ones a dependency created and kept a
    reference to) are wrapped on entry, and ``Client.__new__`` is hooked so
    clients created inside the block are wrapped as they are built, however
    their reader and writer were supplied. Clients are unwrapped, the hook is
    removed, and the ``Client`` singletons are restored on exit.
    """
    from datacustomcode.client import (
        Client,
        StreamingClient,
        _BaseClient,
    )

    if _active["session"] is not None:
        raise RuntimeError("Lineage capture is already active")

    session = _CaptureSession(collector)
    saved_clients = Client._instance, StreamingClient._instance
    original_new = _BaseClient.__dict__["__new__"]
    build = original_new.__func__

    def capturing_new(cls: type, *args: Any, **kwargs: Any) -> _BaseClient:
        client = build(cls, *args, **kwargs)
        session.wrap(client)
        return client  # type: ignore[no-any-return]

    _active["session"] = session
    try:
        for client in saved_clients:
            if client is not None:
                session.wrap(client)
        _BaseClient.__new__ = staticmethod(capturing_new)  # type: ignore[assignment]
        yield collector
    finally:
        _BaseClient.__new__ = original_new  # type: ignore[method-assign]
        session.restore()
        Client._instance, StreamingClient._instance = saved_clients
        _active["session"] = None
