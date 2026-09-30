from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.io import cdf
from datacustomcode.io.writer.base import WriteMode
from datacustomcode.io.writer.local_deltas import LocalDeltasWriter


@pytest.fixture
def writer():
    with patch.object(LocalDeltasWriter, "__init__", return_value=None):
        w = LocalDeltasWriter(spark=None)
        w.spark = MagicMock()
        w._batch_writer = MagicMock()
        yield w


def _configure_writestream_chain(df):
    ws = MagicMock()
    df.writeStream = ws
    ws.foreachBatch.return_value = ws
    ws.option.return_value = ws
    ws.trigger.return_value = ws
    return ws


def test_write_dlo_deltas_uses_trigger_available_now(writer):
    df = MagicMock()
    ws = _configure_writestream_chain(df)

    writer.write_dlo_deltas("Foo__dll", df)

    ws.trigger.assert_called_once_with(availableNow=True)


def test_preview_drops_cdf_metadata_columns(writer):
    df = MagicMock()
    ws = _configure_writestream_chain(df)
    writer.write_dlo_deltas("Foo__dll", df)
    callback = ws.foreachBatch.call_args.args[0]

    # Set up the batch DF so .withColumn().drop() → a mock we can inspect.
    annotated_after_with = MagicMock()
    annotated_after_drop = MagicMock()
    annotated_after_drop.count.return_value = 2
    annotated_after_with.drop.return_value = annotated_after_drop
    batch_df = MagicMock()
    batch_df.withColumn.return_value = annotated_after_with

    # pyspark.sql.functions.{col,when,lit} need a live SparkContext to
    # produce real Columns; stub them out for the pure-Python paths.
    with (
        patch("pyspark.sql.functions.col", MagicMock()),
        patch("pyspark.sql.functions.when", MagicMock()),
        patch("pyspark.sql.functions.lit", MagicMock()),
    ):
        callback(batch_df, 0)

    # drop() removed all three CDF metadata columns.
    assert set(annotated_after_with.drop.call_args.args) == {
        cdf.COMMIT_VERSION,
        cdf.COMMIT_TIMESTAMP,
        cdf.MERGE_RECORD_TYPE,
    }


def test_batch_writes_delegate_to_print_writer(writer):
    df = MagicMock()

    writer.write_to_dlo("Foo__dll", df, WriteMode.APPEND)
    writer.auto_write_to_dlo("Foo__dll", df)

    writer._batch_writer.write_to_dlo.assert_called_once_with(
        "Foo__dll", df, WriteMode.APPEND
    )
    writer._batch_writer.auto_write_to_dlo.assert_called_once_with("Foo__dll", df)
