"""Tests for analyzed-plan column lineage tracing (real local Spark)."""

from __future__ import annotations

import time

import pandas as pd
from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType
import pytest

from datacustomcode.lineage.tracer import (
    UNTRACEABLE_SOURCE,
    LineageTraceError,
    SourceRegistry,
    TraceResult,
)

pytestmark = pytest.mark.spark


@pytest.fixture
def registry():
    return SourceRegistry()


@pytest.fixture
def orders(spark, registry):
    df = spark.createDataFrame(
        pd.DataFrame(
            {
                "id": [1, 2],
                "first": ["a", "b"],
                "last": ["x", "y"],
                "amount": [1.0, 2.0],
                "tags": [["t1", "t2"], ["t3"]],
            }
        )
    )
    registry.register(df, "orders__dll")
    return df


@pytest.fixture
def customers(spark, registry):
    df = spark.createDataFrame(pd.DataFrame({"id": [1, 2], "region": ["us", "eu"]}))
    registry.register(df, "customers__dll")
    return df


def sources(results: dict[str, TraceResult]) -> dict[str, set[str]]:
    assert not [r for r in results.values() if r.unresolved], results
    return {name: set(r.sources) for name, r in results.items()}


def test_passthrough(registry, orders):
    assert sources(registry.trace(orders.select("id", "amount"))) == {
        "id": {"orders__dll.id"},
        "amount": {"orders__dll.amount"},
    }


def test_rename_and_concat(registry, orders):
    df = orders.select(
        F.col("id").alias("order_id"),
        F.concat_ws(" ", "first", "last").alias("full_name"),
    )
    assert sources(registry.trace(df)) == {
        "order_id": {"orders__dll.id"},
        "full_name": {"orders__dll.first", "orders__dll.last"},
    }


def test_filter_and_join_keys_are_not_sources(registry, orders, customers):
    df = (
        orders.filter(F.col("amount") > 1)
        .join(customers, orders.id == customers.id)
        .select(orders["first"], customers["region"])
    )
    assert sources(registry.trace(df)) == {
        "first": {"orders__dll.first"},
        "region": {"customers__dll.region"},
    }


def test_using_join_key_is_coalesced(registry, orders, customers):
    df = orders.join(customers, "id", "full_outer").select("id")
    assert sources(registry.trace(df)) == {
        "id": {"orders__dll.id", "customers__dll.id"}
    }


def test_python_udf_inputs(registry, orders):
    shout = F.udf(lambda a, b: f"{a}{b}".upper(), StringType())
    df = orders.select(shout("first", "last").alias("loud"))
    assert sources(registry.trace(df)) == {
        "loud": {"orders__dll.first", "orders__dll.last"}
    }


def test_aggregate(registry, orders):
    df = orders.groupBy("first").agg(F.sum("amount").alias("total"))
    assert sources(registry.trace(df)) == {
        "first": {"orders__dll.first"},
        "total": {"orders__dll.amount"},
    }


def test_literal_has_no_sources(registry, orders):
    results = registry.trace(orders.select(F.lit("const").alias("c")))
    assert results["c"] == TraceResult()


def test_union_is_positional(registry, orders, customers):
    df = orders.select("id", "first").union(customers.select("id", "region"))
    assert sources(registry.trace(df)) == {
        "id": {"orders__dll.id", "customers__dll.id"},
        "first": {"orders__dll.first", "customers__dll.region"},
    }


def test_union_by_name(registry, orders, customers):
    df = orders.select("id", F.col("first").alias("region")).unionByName(
        customers.select("region", "id")
    )
    assert sources(registry.trace(df)) == {
        "id": {"orders__dll.id", "customers__dll.id"},
        "region": {"orders__dll.first", "customers__dll.region"},
    }


def test_window(registry, orders):
    window = Window.partitionBy("first").orderBy("amount")
    df = orders.select("id", F.sum("amount").over(window).alias("running"))
    assert sources(registry.trace(df)) == {
        "id": {"orders__dll.id"},
        "running": {"orders__dll.amount", "orders__dll.first"},
    }


def test_explode(registry, orders):
    df = orders.select("id", F.explode("tags").alias("tag"))
    assert sources(registry.trace(df)) == {
        "id": {"orders__dll.id"},
        "tag": {"orders__dll.tags"},
    }


def test_rollup(registry, orders):
    df = orders.rollup("first").agg(F.sum("amount").alias("total"))
    assert sources(registry.trace(df)) == {
        "first": {"orders__dll.first"},
        "total": {"orders__dll.amount"},
    }


def test_self_join(registry, orders):
    left, right = orders.alias("l"), orders.alias("r")
    df = left.join(right, F.col("l.id") == F.col("r.id")).select(
        F.col("l.first").alias("left_first"), F.col("r.last").alias("right_last")
    )
    assert sources(registry.trace(df)) == {
        "left_first": {"orders__dll.first"},
        "right_last": {"orders__dll.last"},
    }


def test_dropped_then_readded(registry, orders):
    df = orders.drop("first").withColumn("first", F.upper("last"))
    assert sources(registry.trace(df.select("first"))) == {
        "first": {"orders__dll.last"}
    }


def test_to_pandas_round_trip_is_untraceable(spark, registry, orders):
    df = spark.createDataFrame(orders.select("id").toPandas())
    assert registry.trace(df)["id"] == TraceResult(reason=UNTRACEABLE_SOURCE)


def test_unregistered_dataframe_is_untraceable(spark, registry, orders):
    lookup = spark.createDataFrame(pd.DataFrame({"id": [1], "label": ["one"]}))
    df = orders.join(lookup, "id").select("first", "label")
    results = registry.trace(df)
    assert results["first"].sources == {"orders__dll.first"}
    assert results["label"] == TraceResult(reason=UNTRACEABLE_SOURCE)


def test_opaque_operator_is_unresolved(registry, orders):
    def identity(batches):
        yield from batches

    df = orders.select("id").mapInPandas(identity, "id long")
    assert registry.trace(df)["id"].unresolved


def test_scalar_subquery_is_unresolved(spark, registry, orders):
    orders.createOrReplaceTempView("orders_view")
    df = spark.sql(
        "SELECT id, (SELECT max(amount) FROM orders_view) AS top FROM orders_view"
    )
    results = registry.trace(df)
    assert results["id"].sources == {"orders__dll.id"}
    assert results["top"].unresolved


def test_wide_passthrough_is_fast(spark):
    columns = [f"field_{i:04d}__c" for i in range(512)]
    wide = spark.createDataFrame(pd.DataFrame([list(range(512))], columns=columns))
    registry = SourceRegistry()
    registry.register(wide, "wide__dll")

    start = time.monotonic()
    results = sources(registry.trace(wide.select(*columns)))
    elapsed = time.monotonic() - start

    assert results == {c: {f"wide__dll.{c}"} for c in columns}
    assert elapsed < 1.0


def test_spark_connect_dataframe_rejected(registry):
    class ConnectLike:
        pass

    with pytest.raises(LineageTraceError, match="Spark Connect"):
        registry.trace(ConnectLike())  # type: ignore[arg-type]
