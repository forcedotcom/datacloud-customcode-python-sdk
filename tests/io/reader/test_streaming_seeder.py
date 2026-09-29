from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from datacustomcode.io import cdf
from datacustomcode.io.reader.streaming_seeder import StreamingSourceSeeder


SOURCE_SCHEMA = StructType(
    [
        StructField("id__c", StringType(), True),
        StructField("age__c", LongType(), True),
    ]
)


def _stub_read_result(rows_df: pd.DataFrame):
    limited = MagicMock()
    limited.toPandas.return_value = rows_df
    limited.schema = SOURCE_SCHEMA

    df = MagicMock()
    df.limit.return_value = limited
    return df


@pytest.fixture
def seeder():
    """Seeder with __init__ short-circuited (avoids real Query API creds)."""
    with patch.object(StreamingSourceSeeder, "__init__", return_value=None):
        s = StreamingSourceSeeder(spark=None)
        s.spark = MagicMock()
        s.reader = MagicMock()
        yield s


def test_non_empty_source_writes_schema_and_seed(seeder, tmp_path):
    seeder.reader.read_dlo.return_value = _stub_read_result(
        pd.DataFrame(
            [{"id__c": str(i), "age__c": i} for i in range(4)]
        )
    )

    wrote = seeder.seed_source("Foo__dll", "dlo", str(tmp_path))

    assert wrote is True
    out_dir = tmp_path / "Foo__dll"
    assert (out_dir / "_schema.json").exists()
    assert (out_dir / "000_seed.json").exists()

    rows = [
        json.loads(line)
        for line in (out_dir / "000_seed.json").read_text().splitlines()
    ]
    for row in rows:
        assert cdf.COMMIT_VERSION in row
        assert cdf.COMMIT_TIMESTAMP in row
        assert cdf.MERGE_RECORD_TYPE in row
    # Both operation types appear in the seed.
    ops = {row[cdf.MERGE_RECORD_TYPE] for row in rows}
    assert ops == {
        cdf.MergeRecordType.UPSERT.value,
        cdf.MergeRecordType.DELETE.value,
    }


def test_empty_source_returns_false_and_writes_nothing(seeder, tmp_path):
    seeder.reader.read_dlo.return_value = _stub_read_result(pd.DataFrame([]))

    wrote = seeder.seed_source("Foo__dll", "dlo", str(tmp_path))

    assert wrote is False
    assert not (tmp_path / "Foo__dll").exists()
