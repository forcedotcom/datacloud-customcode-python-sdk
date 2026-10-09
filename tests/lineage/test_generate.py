"""End-to-end tests for generate_lineage with an in-memory reader (no org)."""

from __future__ import annotations

import json
import sys
import textwrap

import pandas as pd
import pytest
import yaml

from datacustomcode.client import (
    Client,
    StreamingClient,
    _BaseClient,
)
from datacustomcode.config import config
from datacustomcode.io.reader.base import BaseDataCloudReader
from datacustomcode.io.writer.base import BaseDataCloudWriter, WriteMode
from datacustomcode.lineage import capture_lineage, generate_lineage
from datacustomcode.lineage.capture import LineageCollector
from datacustomcode.scan import SDK_CONFIG_DIR, SDK_CONFIG_FILE

pytestmark = pytest.mark.spark

ORIGINAL_NEW = _BaseClient.__dict__["__new__"]

TABLES = {
    "orders__dll": pd.DataFrame(
        {"id": [1, 2], "first": ["a", "b"], "last": ["x", "y"], "amount": [1.0, 2.0]}
    ),
    "customers__dlm": pd.DataFrame({"id": [1, 2], "region": ["us", "eu"]}),
}
VALIDATED: list[tuple[str, list[str]]] = []


class InMemoryReader(BaseDataCloudReader):
    CONFIG_NAME = "LineageTestInMemoryReader"

    def __init__(self, spark, dataspace=None):
        super().__init__(spark)

    def read_dlo(self, name, schema=None):
        return self.spark.createDataFrame(TABLES[name])

    read_dmo = read_dlo


class RefusingWriter(BaseDataCloudWriter):
    CONFIG_NAME = "LineageTestRefusingWriter"

    def __init__(self, spark, dataspace=None):
        super().__init__(spark)

    def validate_dataframe_columns_against_dlo(self, dataframe, dlo_name):
        VALIDATED.append((dlo_name, list(dataframe.columns)))

    def write_to_dlo(self, name, dataframe, write_mode):
        raise AssertionError("lineage generation must never write")

    write_to_dmo = write_to_dlo


@pytest.fixture(autouse=True)
def isolated_client(spark):
    saved = (config.reader_config, config.writer_config, config.spark_config)
    saved_spark = _BaseClient._shared_spark
    Client._instance = StreamingClient._instance = None
    _BaseClient._shared_spark = spark
    VALIDATED.clear()
    yield
    config.reader_config, config.writer_config, config.spark_config = saved
    Client._instance = StreamingClient._instance = None
    _BaseClient._shared_spark = saved_spark


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


def make_package(tmp_path, body: str, config_json=None, package_type=None) -> str:
    payload = tmp_path / "payload"
    payload.mkdir()
    entrypoint = payload / "entrypoint.py"
    entrypoint.write_text(
        "from pyspark.sql import functions as F\n"
        "from datacustomcode.client import Client\n"
        "from datacustomcode.io.writer.base import WriteMode\n"
        "client = Client()\n" + textwrap.dedent(body)
    )
    (payload / "config.json").write_text(
        json.dumps(config_json or {"dataspace": "default"})
    )
    if package_type:
        sdk_dir = tmp_path / SDK_CONFIG_DIR
        sdk_dir.mkdir()
        (sdk_dir / SDK_CONFIG_FILE).write_text(json.dumps({"type": package_type}))
    return str(entrypoint)


def test_generate_lineage_end_to_end(tmp_path, config_file):
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
    assert VALIDATED == [("out__dll", ["id__c", "name__c", "const__c", "lookup_id__c"])]


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


def test_dmo_read_and_write(tmp_path, config_file):
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
    assert VALIDATED == []


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


def test_script_error_propagates_and_restores_state(tmp_path, config_file):
    config.load(config_file)
    outside = Client()
    reader, writer = outside._reader, outside._writer
    entrypoint = make_package(tmp_path, "raise RuntimeError('boom')\n")

    with pytest.raises(RuntimeError, match="boom"):
        generate_lineage(entrypoint, config_file)

    assert config.reader_config.type_config_name == InMemoryReader.CONFIG_NAME
    assert config.writer_config.type_config_name == RefusingWriter.CONFIG_NAME
    assert Client._instance is outside
    assert (outside._reader, outside._writer) == (reader, writer)
    assert "__new__" not in vars(Client)
    assert _BaseClient.__dict__["__new__"] is ORIGINAL_NEW


def test_existing_client_is_captured_and_restored(spark, config_file):
    config.load(config_file)
    outside = Client()
    writer = outside._writer

    with capture_lineage(LineageCollector()):
        assert Client() is outside
        assert type(outside._writer).__name__ == "LineageCaptureWriter"
        assert outside._writer._delegate is writer

    assert outside._writer is writer
    assert Client() is outside


def test_retained_client_from_dependency_is_captured(
    tmp_path, config_file, monkeypatch
):
    # A dependency that builds its Client at import time keeps it in a module
    # global; its writes must still be captured, never reach the real writer
    (tmp_path / "lineage_dep_helpers.py").write_text(
        "from datacustomcode.client import Client\n"
        "from datacustomcode.io.writer.base import WriteMode\n"
        "client = Client()\n"
        "def save(df):\n"
        "    client.write_to_dlo('out__dll', df, WriteMode.APPEND)\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "lineage_dep_helpers", raising=False)
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
    assert VALIDATED == [("out__dll", ["id__c"])]


def test_client_built_with_reader_and_writer_is_captured(spark, config_file):
    config.load(config_file)
    reader, writer = InMemoryReader(spark), RefusingWriter(spark)
    collector = LineageCollector()

    with capture_lineage(collector):
        client = Client(reader=reader, writer=writer)
        assert client._reader._delegate is reader
        assert client._writer._delegate is writer
        client.write_to_dlo(
            "out__dll",
            client.read_dlo("orders__dll").select("id"),
            WriteMode.APPEND,
        )

    assert collector.result().data_lineage == {"out__dll.id": ["orders__dll.id"]}
    assert (client._reader, client._writer) == (reader, writer)


def test_nested_capture_is_rejected(config_file):
    with capture_lineage(LineageCollector()):
        with pytest.raises(RuntimeError, match="already active"):
            with capture_lineage(LineageCollector()):
                pass


def test_capture_falls_through_to_delegate_attributes(config_file):
    config.load(config_file)

    with capture_lineage(LineageCollector()):
        writer = Client()._writer
        assert writer.validate_dataframe_columns_against_dlo is not None
        assert isinstance(writer._delegate, RefusingWriter)
