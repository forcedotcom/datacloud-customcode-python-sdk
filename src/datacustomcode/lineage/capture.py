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

:func:`install_capture` replaces the configured reader and writer with forced
configs for :class:`LineageCaptureReader` and :class:`LineageCaptureWriter`, so
every client built afterwards uses them, even one passed its own
``Client(reader=..., writer=...)``. The wrappers delegate to the reader/writer
the config resolved, so the customer's reader choice and credentials still
apply: reads go through the delegate and are registered as lineage sources,
writes are validated through the delegate (when it supports it), traced, and
never written.

Forcing config only reaches clients built after it, so capture runs in a fresh
process (see :mod:`datacustomcode.lineage._worker`), where no client exists yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
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
    from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
    from pyspark.sql.types import AtomicType, StructType

    from datacustomcode.config import AccessLayerObjectConfig


@dataclass
class LineageResult:
    """Field-level lineage captured from one run of an entrypoint.

    Attributes:
        data_lineage: Target field to its sorted source fields.
        unresolved: Target field to the reason its lineage could not be traced.
        no_sources: Target fields computed from no source field (e.g. literals).
        udf_fields: Target fields in ``data_lineage`` or ``no_sources`` computed
            by a user-defined function, whose sources may be incomplete.
        targets: Objects written.
        sources: Objects read.
    """

    data_lineage: Dict[str, List[str]] = field(default_factory=dict)
    unresolved: Dict[str, str] = field(default_factory=dict)
    no_sources: List[str] = field(default_factory=list)
    udf_fields: List[str] = field(default_factory=list)
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
            "udfFields": sorted(self.udf_fields),
            "targets": sorted(self.targets),
            "sources": sorted(self.sources),
            "pairCount": self.pair_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LineageResult:
        """Inverse of :meth:`to_dict`."""
        return cls(
            data_lineage={
                entry["targetField"]: list(entry["sourceFields"])
                for entry in data["dataLineage"]
            },
            unresolved={
                entry["targetField"]: entry["reason"] for entry in data["unresolved"]
            },
            no_sources=list(data["noSources"]),
            udf_fields=list(data["udfFields"]),
            targets=list(data["targets"]),
            sources=list(data["sources"]),
        )


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
            # every write, stays unresolved if any write was unresolved, and is
            # UDF-computed if any write computed it with a UDF
            previous = fields.get(column)
            if previous is None or (traced.unresolved and not previous.unresolved):
                fields[column] = traced
            elif not previous.unresolved:
                fields[column] = TraceResult(
                    sources=previous.sources | traced.sources,
                    udf=previous.udf or traced.udf,
                )

    def result(self) -> LineageResult:
        result = LineageResult(
            targets=list(self._targets), sources=list(self.registry.objects)
        )
        for object_name, fields in self._targets.items():
            for column, traced in fields.items():
                target = f"{object_name}.{column}"
                if traced.reason is not None:
                    result.unresolved[target] = traced.reason
                    continue
                if traced.sources:
                    result.data_lineage[target] = sorted(traced.sources)
                else:
                    result.no_sources.append(target)
                if traced.udf:
                    result.udf_fields.append(target)
        return result


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

    def __init__(
        self,
        spark: SparkSession,
        delegate_config: AccessLayerObjectConfig[BaseDataCloudReader],
        collector: LineageCollector,
    ) -> None:
        super().__init__(spark)
        self._delegate = delegate_config.to_object(spark)
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

    CONFIG_NAME = "LineageCaptureWriter"

    def __init__(
        self,
        spark: SparkSession,
        delegate_config: AccessLayerObjectConfig[BaseDataCloudWriter],
        collector: LineageCollector,
    ) -> None:
        super().__init__(spark)
        self._delegate = delegate_config.to_object(spark)
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


def install_capture(collector: LineageCollector) -> None:
    """Force every client built from now on to capture lineage into ``collector``.

    Wraps the current ``config.reader_config`` and ``config.writer_config`` (as
    resolved by ``prepare_run``) in forced capture configs. Clients that already
    exist are not affected, which is why capture runs in a fresh process.

    Raises:
        ValueError: If no reader or writer is configured.
    """
    from datacustomcode.config import AccessLayerObjectConfig, config

    if config.reader_config is None or config.writer_config is None:
        raise ValueError("Lineage generation needs a reader_config and writer_config")
    config.reader_config = AccessLayerObjectConfig(
        type_config_name=LineageCaptureReader.CONFIG_NAME,
        options={"delegate_config": config.reader_config, "collector": collector},
        force=True,
    )
    config.writer_config = AccessLayerObjectConfig(
        type_config_name=LineageCaptureWriter.CONFIG_NAME,
        options={"delegate_config": config.writer_config, "collector": collector},
        force=True,
    )
