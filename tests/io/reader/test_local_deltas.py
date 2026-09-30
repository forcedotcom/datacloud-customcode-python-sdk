from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)
import pytest

from datacustomcode.io import cdf
from datacustomcode.io.reader.local_deltas import LocalDeltasReader

SOURCE_SCHEMA = StructType(
    [
        StructField("id__c", StringType(), True),
        StructField("age__c", LongType(), True),
    ]
)


@pytest.fixture
def reader(tmp_path):
    """Reader with __init__ short-circuited (avoids Query API construction)."""
    with patch.object(LocalDeltasReader, "__init__", return_value=None):
        r = LocalDeltasReader(spark=None)
        r.spark = MagicMock()
        r._fixtures_root = tmp_path
        r._credentials_profile = "default"
        r._dataspace = None
        r._sf_cli_org = None
        r._batch = MagicMock()
        r._current_layer = "dlo"
        yield r


def _write_schema_file(fixtures_root, name):
    out_dir = fixtures_root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "_schema.json").write_text(json.dumps(SOURCE_SCHEMA.jsonValue()))


def test_stream_schema_is_source_columns_plus_three_cdf_columns(reader, tmp_path):
    _write_schema_file(tmp_path, "Foo__dll")

    schema = reader._build_stream_schema("Foo__dll")

    names = [f.name for f in schema.fields]
    assert names == [
        "id__c",
        "age__c",
        cdf.COMMIT_VERSION,
        cdf.COMMIT_TIMESTAMP,
        cdf.MERGE_RECORD_TYPE,
    ]


def test_open_stream_invokes_seeder_when_schema_file_missing(reader, tmp_path):
    reader.spark.readStream.format.return_value = reader.spark.readStream
    reader.spark.readStream.schema.return_value = reader.spark.readStream
    reader.spark.readStream.option.return_value = reader.spark.readStream
    reader.spark.readStream.load.return_value = MagicMock()

    def fake_seed(name):
        _write_schema_file(tmp_path, name)
        return True

    with patch.object(reader, "_try_seed", side_effect=fake_seed) as try_seed:
        reader._open_stream("Foo__dll")
        try_seed.assert_called_once_with("Foo__dll")


def test_open_stream_exits_cleanly_on_empty_source(reader):
    with patch.object(reader, "_try_seed", return_value=False):
        with pytest.raises(SystemExit) as exc:
            reader._open_stream("Foo__dll")
        assert exc.value.code == 0
        reader.spark.readStream.format.assert_not_called()


def test_read_dlo_delegates_to_batch_reader(reader):
    reader.read_dlo("Foo__dll", None)
    reader._batch.read_dlo.assert_called_once_with("Foo__dll", None)
