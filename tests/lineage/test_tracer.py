"""Tests for analyzed-plan column lineage tracing (real local Spark)."""

from __future__ import annotations

import time

import pandas as pd
from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType
import pytest

from datacustomcode.lineage.tracer import (
    AMBIGUOUS_SOURCE,
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
    results = registry.trace(df)
    assert sources(results) == {"loud": {"orders__dll.first", "orders__dll.last"}}
    assert results["loud"].udf


def test_udf_flag_marks_only_udf_columns(registry, orders):
    shout = F.udf(lambda a: a.upper(), StringType())
    df = orders.select("id", F.upper("last").alias("up"), shout("first").alias("s"))
    results = registry.trace(df)
    assert {name for name, r in results.items() if r.udf} == {"s"}


def test_pandas_udf_is_flagged(registry, orders):
    @F.pandas_udf("double")
    def double_it(s: pd.Series) -> pd.Series:
        return s * 2

    results = registry.trace(orders.select(double_it("amount").alias("dbl")))
    assert results["dbl"] == TraceResult(
        sources=frozenset({"orders__dll.amount"}), udf=True
    )


def test_grouped_agg_pandas_udf_is_flagged(registry, orders):
    @F.pandas_udf("double")
    def total(s: pd.Series) -> float:
        return float(s.sum())

    results = registry.trace(orders.groupBy("first").agg(total("amount").alias("t")))
    assert sources(results) == {
        "first": {"orders__dll.first"},
        "t": {"orders__dll.amount"},
    }
    assert results["t"].udf
    assert not results["first"].udf


def test_sql_registered_udf_is_flagged(spark, registry, orders):
    spark.udf.register("lineage_shout", lambda a: a.upper(), StringType())
    orders.createOrReplaceTempView("orders_udf_view")
    df = spark.sql("SELECT id, lineage_shout(first) AS s FROM orders_udf_view")
    results = registry.trace(df)
    assert results["s"] == TraceResult(
        sources=frozenset({"orders__dll.first"}), udf=True
    )
    assert not results["id"].udf


def test_udf_flag_survives_rename_and_later_expressions(registry, orders):
    shout = F.udf(lambda a: a.upper(), StringType())
    df = (
        orders.select(shout("first").alias("s"), "last")
        .withColumnRenamed("s", "renamed")
        .withColumn("joined", F.concat("renamed", "last"))
    )
    results = registry.trace(df)
    assert results["renamed"].udf
    assert results["joined"] == TraceResult(
        sources=frozenset({"orders__dll.first", "orders__dll.last"}), udf=True
    )
    assert not results["last"].udf


def test_udf_without_arguments_has_no_sources_but_is_flagged(registry, orders):
    constant = F.udf(lambda: "x", StringType())
    results = registry.trace(orders.select(constant().alias("c")))
    assert results["c"] == TraceResult(udf=True)


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


def test_derived_reader_column_does_not_rename_its_source(spark, registry):
    # A reader that exposes both a column and a value computed from it: the
    # computed column must not take over the source column's field identity
    base = spark.createDataFrame(pd.DataFrame({"name": ["a", "b"]}))
    source = base.select("name", F.upper("name").alias("normalized"))
    registry.register(source, "source__dll")

    results = registry.trace(
        source.select(
            F.col("name").alias("name__c"), F.col("normalized").alias("norm__c")
        )
    )

    assert sources(results) == {
        "name__c": {"source__dll.name"},
        "norm__c": {"source__dll.name"},
    }


def test_renamed_reader_column_names_the_field(spark, registry):
    base = spark.createDataFrame(pd.DataFrame({"raw": ["a"]}))
    registry.register(base.select(F.col("raw").alias("name")), "source__dll")

    traced = registry.trace(base.select(F.col("raw").alias("name__c")))

    assert sources(traced) == {"name__c": {"source__dll.name"}}


def test_two_objects_over_one_relation_are_ambiguous(spark, registry):
    base = spark.createDataFrame(pd.DataFrame({"id": [1], "other": [2]}))
    registry.register(base, "first__dll")
    registry.register(base, "second__dll")

    results = registry.trace(base.select("id"))

    assert results["id"].reason == AMBIGUOUS_SOURCE
    assert results["id"].unresolved


def test_one_leaf_under_two_names_is_ambiguous(spark, registry):
    base = spark.createDataFrame(pd.DataFrame({"id": [1]}))
    registry.register(base.select("id", F.col("id").alias("id_copy")), "src__dll")

    assert registry.trace(base.select("id"))["id"].reason == AMBIGUOUS_SOURCE


def test_stack_outputs_trace_positionally(registry, orders):
    stacked = orders.selectExpr("stack(1, first, last) AS (x, y)")
    assert sources(registry.trace(stacked)) == {
        "x": {"orders__dll.first"},
        "y": {"orders__dll.last"},
    }


def test_stack_with_several_rows(registry, orders):
    stacked = orders.selectExpr(
        "stack(2, first, amount, last, CAST(id AS DOUBLE)) AS (name, n)"
    )
    assert sources(registry.trace(stacked)) == {
        "name": {"orders__dll.first", "orders__dll.last"},
        "n": {"orders__dll.id", "orders__dll.amount"},
    }


def test_inline_over_built_structs_traces_each_field(registry, orders):
    inlined = orders.selectExpr(
        "inline(array(named_struct('a', first, 'b', last), "
        "named_struct('a', last, 'b', 'constant'))) AS (x, y)"
    )
    assert sources(registry.trace(inlined)) == {
        "x": {"orders__dll.first", "orders__dll.last"},
        "y": {"orders__dll.last"},
    }


def test_inline_literal_field_has_no_sources(spark, registry):
    df = spark.createDataFrame(pd.DataFrame({"name": ["a"]}))
    registry.register(df, "src__dll")
    inlined = df.selectExpr("inline(array(struct(name, 'constant'))) AS (x, y)")
    assert sources(registry.trace(inlined)) == {"x": {"src__dll.name"}, "y": set()}


def test_explode_of_built_map_traces_key_and_value(spark, registry):
    df = spark.createDataFrame(pd.DataFrame({"name": ["a"]}))
    registry.register(df, "src__dll")
    assert sources(registry.trace(df.selectExpr("explode(map('key', name))"))) == {
        "key": set(),
        "value": {"src__dll.name"},
    }
    exploded = df.selectExpr("posexplode(map('key', name)) AS (p, k, v)")
    assert sources(registry.trace(exploded)) == {
        "p": {"src__dll.name"},
        "k": set(),
        "v": {"src__dll.name"},
    }


def test_inline_over_source_struct_column_shares_its_trace(spark, registry):
    df = spark.createDataFrame([([("a", 1)],)], "items array<struct<s:string,n:int>>")
    registry.register(df, "src__dll")
    assert sources(registry.trace(df.selectExpr("inline(items)"))) == {
        "s": {"src__dll.items"},
        "n": {"src__dll.items"},
    }


@pytest.mark.parametrize(
    "build, generate",
    [
        # The struct is built in an earlier projection, hidden behind a column
        (
            "array(struct(name, 'constant')) AS items",
            "inline(items) AS (x, y)",
        ),
        # The struct is built inline but wrapped in another function
        (
            "name",
            "inline(filter(array(struct(name, 'constant')), s -> true)) AS (x, y)",
        ),
        ("map('key', name) AS m", "explode(m) AS (k, v)"),
    ],
)
def test_generator_over_computed_collection_is_unresolved(
    spark, registry, build, generate
):
    df = spark.createDataFrame(pd.DataFrame({"name": ["a"]}))
    registry.register(df, "src__dll")
    results = registry.trace(df.selectExpr(build).selectExpr(generate))
    assert all(r.unresolved for r in results.values()), results
    assert "cannot be traced" in next(iter(results.values())).reason


def test_posexplode_outputs_share_their_single_input(registry, orders):
    exploded = orders.select(F.posexplode("tags").alias("pos", "tag"))
    assert sources(registry.trace(exploded)) == {
        "pos": {"orders__dll.tags"},
        "tag": {"orders__dll.tags"},
    }
