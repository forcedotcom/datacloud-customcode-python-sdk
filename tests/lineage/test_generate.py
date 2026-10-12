"""End-to-end tests for generate_lineage with an in-memory reader (no org)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import textwrap

import pytest
import yaml

from datacustomcode.config import AccessLayerObjectConfig
from datacustomcode.lineage import LineageResult, generate_lineage
from datacustomcode.lineage.capture import LineageCaptureWriter, LineageCollector
from datacustomcode.scan import SDK_CONFIG_DIR, SDK_CONFIG_FILE

# generate_lineage passes sys.path to its worker, so the worker can import this too
sys.path.insert(0, str(Path(__file__).parent))
from lineage_test_io import (
    VALIDATED_FILE_ENV,
    InMemoryReader,
    RefusingWriter,
)

pytestmark = pytest.mark.spark

PREAMBLE = (
    "from pyspark.sql import functions as F\n"
    "import lineage_test_io\n"
    "from datacustomcode.client import Client\n"
    "from datacustomcode.io.writer.base import WriteMode\n"
)


@pytest.fixture(autouse=True)
def validated(tmp_path, monkeypatch):
    """Reads back the (dlo, columns) validations the worker's writer recorded."""
    path = tmp_path / "validated.jsonl"
    monkeypatch.setenv(VALIDATED_FILE_ENV, str(path))

    def read():
        if not path.exists():
            return []
        return [tuple(json.loads(line)) for line in path.read_text().splitlines()]

    return read


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.dump(
            {
                "reader_config": {"type_config_name": InMemoryReader.CONFIG_NAME},
                "writer_config": {"type_config_name": RefusingWriter.CONFIG_NAME},
            }
        )
    )
    return str(path)


def make_package(
    tmp_path, body: str, config_json=None, package_type=None, with_client=True
) -> str:
    payload = tmp_path / "payload"
    payload.mkdir()
    entrypoint = payload / "entrypoint.py"
    preamble = PREAMBLE + ("client = Client()\n" if with_client else "")
    entrypoint.write_text(preamble + textwrap.dedent(body))
    (payload / "config.json").write_text(
        json.dumps(config_json or {"dataspace": "default"})
    )
    if package_type:
        sdk_dir = tmp_path / SDK_CONFIG_DIR
        sdk_dir.mkdir()
        (sdk_dir / SDK_CONFIG_FILE).write_text(json.dumps({"type": package_type}))
    return str(entrypoint)


def test_generate_lineage_end_to_end(tmp_path, config_file, validated):
    entrypoint = make_package(
        tmp_path,
        """
        orders = client.read_dlo("orders__dll")
        lookup = orders.sparkSession.createDataFrame(orders.select("id").toPandas())
        df = orders.join(lookup, "id").select(
            F.col("id").alias("id__c"),
            F.concat_ws(" ", "first", "last").alias("name__c"),
            F.lit("x").alias("const__c"),
            lookup["id"].alias("lookup_id__c"),
        )
        client.write_to_dlo("out__dll", df, WriteMode.APPEND)
        """,
    )

    result = generate_lineage(entrypoint, config_file)

    assert result.to_dict() == {
        "dataLineage": [
            {"targetField": "out__dll.id__c", "sourceFields": ["orders__dll.id"]},
            {
                "targetField": "out__dll.name__c",
                "sourceFields": ["orders__dll.first", "orders__dll.last"],
            },
        ],
        "unresolved": [
            {"targetField": "out__dll.lookup_id__c", "reason": "untraceable source"}
        ],
        "noSources": ["out__dll.const__c"],
        "targets": ["out__dll"],
        "sources": ["orders__dll"],
        "pairCount": 3,
    }
    assert validated() == [
        ("out__dll", ["id__c", "name__c", "const__c", "lookup_id__c"])
    ]


def test_repeat_writes_union_sources(tmp_path, config_file):
    entrypoint = make_package(
        tmp_path,
        """
        orders = client.read_dlo("orders__dll")
        client.write_to_dlo(
            "out__dll", orders.select(F.col("first").alias("v__c")), WriteMode.APPEND
        )
        client.write_to_dlo(
            "out__dll", orders.select(F.col("last").alias("v__c")), WriteMode.APPEND
        )
        """,
    )

    lineage = generate_lineage(entrypoint, config_file).to_dict()["dataLineage"]

    assert lineage == [
        {
            "targetField": "out__dll.v__c",
            "sourceFields": ["orders__dll.first", "orders__dll.last"],
        }
    ]


def test_dmo_read_and_write(tmp_path, config_file, validated):
    entrypoint = make_package(
        tmp_path,
        """
        df = client.read_dmo("customers__dlm").select(
            F.upper("region").alias("region__c")
        )
        client.write_to_dmo("out__dlm", df, WriteMode.APPEND)
        """,
    )

    result = generate_lineage(entrypoint, config_file)

    assert result.data_lineage == {"out__dlm.region__c": ["customers__dlm.region"]}
    assert validated() == []


def test_rejects_function_packages(tmp_path, config_file):
    entrypoint = make_package(tmp_path, "", package_type="function")

    with pytest.raises(ValueError, match="only be generated for script packages"):
        generate_lineage(entrypoint, config_file)


def test_rejects_streaming_scripts(tmp_path, config_file):
    entrypoint = make_package(
        tmp_path,
        "",
        config_json={"dataspace": "default", "streamingSource": {"name": "a__dll"}},
    )

    with pytest.raises(ValueError, match="does not support streaming"):
        generate_lineage(entrypoint, config_file)


def test_script_error_propagates(tmp_path, config_file):
    entrypoint = make_package(tmp_path, "raise RuntimeError('boom')\n")

    with pytest.raises(RuntimeError, match="boom"):
        generate_lineage(entrypoint, config_file)


def test_non_builtin_error_is_reported_by_name(tmp_path, config_file):
    entrypoint = make_package(
        tmp_path,
        """
        class CustomerError(Exception):
            pass
        raise CustomerError("bad row")
        """,
    )

    with pytest.raises(RuntimeError, match="CustomerError: bad row"):
        generate_lineage(entrypoint, config_file)


def test_worker_exit_without_response_is_reported(tmp_path, config_file):
    entrypoint = make_package(tmp_path, "import os\nos._exit(3)\n")

    with pytest.raises(RuntimeError, match="exited with code 3"):
        generate_lineage(entrypoint, config_file)


def test_retained_client_from_dependency_is_captured(
    tmp_path, config_file, monkeypatch, validated
):
    # A dependency that builds its Client at import time keeps it in a module
    # global; its writes must still be captured, never reach the real writer
    (tmp_path / "lineage_dep_helpers.py").write_text(
        "import lineage_test_io  # noqa: F401  registers the test reader/writer\n"
        "from datacustomcode.client import Client\n"
        "from datacustomcode.io.writer.base import WriteMode\n"
        "client = Client()\n"
        "def save(df):\n"
        "    client.write_to_dlo('out__dll', df, WriteMode.APPEND)\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    entrypoint = make_package(
        tmp_path,
        """
        import lineage_dep_helpers
        lineage_dep_helpers.save(
            client.read_dlo("orders__dll").select(F.col("id").alias("id__c"))
        )
        """,
    )

    result = generate_lineage(
        entrypoint, config_file, dependencies=["lineage_dep_helpers"]
    )

    assert result.data_lineage == {"out__dll.id__c": ["orders__dll.id"]}
    assert validated() == [("out__dll", ["id__c"])]


def test_repeated_generation_is_independent(tmp_path, config_file, validated):
    entrypoint = make_package(
        tmp_path,
        """
        client.write_to_dlo(
            "out__dll",
            client.read_dlo("orders__dll").select(F.col("id").alias("id__c")),
            WriteMode.APPEND,
        )
        """,
    )

    for _ in range(2):
        result = generate_lineage(entrypoint, config_file)
        assert result.data_lineage == {"out__dll.id__c": ["orders__dll.id"]}
    assert validated() == [("out__dll", ["id__c"])] * 2


def test_client_built_with_explicit_reader_and_writer_is_captured(
    tmp_path, config_file
):
    # force=True overrides even a reader/writer passed straight to Client; the
    # writer here would raise if it were used
    entrypoint = make_package(
        tmp_path,
        """
        class ExplodingWriter(lineage_test_io.RefusingWriter):
            CONFIG_NAME = "LineageTestExplodingWriter"

            def validate_dataframe_columns_against_dlo(self, dataframe, dlo_name):
                raise AssertionError("explicit writer must not be used")

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        client = Client(
            reader=lineage_test_io.InMemoryReader(spark),
            writer=ExplodingWriter(spark),
        )
        client.write_to_dlo(
            "out__dll", client.read_dlo("orders__dll").select("id"), WriteMode.APPEND
        )
        """,
        with_client=False,
    )

    result = generate_lineage(entrypoint, config_file)

    assert result.data_lineage == {"out__dll.id": ["orders__dll.id"]}


def test_capture_falls_through_to_delegate_attributes(spark):
    writer = LineageCaptureWriter(
        spark,
        AccessLayerObjectConfig(type_config_name=RefusingWriter.CONFIG_NAME),
        LineageCollector(),
    )

    assert writer.validate_dataframe_columns_against_dlo is not None
    assert isinstance(writer._delegate, RefusingWriter)


def test_result_round_trips_through_dict():
    result = LineageResult(
        data_lineage={"out__dll.a__c": ["in__dll.a", "in__dll.b"]},
        unresolved={"out__dll.b__c": "untraceable source"},
        no_sources=["out__dll.c__c"],
        targets=["out__dll"],
        sources=["in__dll"],
    )

    assert LineageResult.from_dict(result.to_dict()).to_dict() == result.to_dict()
