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
"""Tests for the column-casing hint hooks."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest

from datacustomcode.spark.column_hints import install_column_casing_hints

HINT_FRAGMENT = "Data Cloud columns must be lowercase"


@pytest.fixture(scope="module")
def spark():
    # Case-sensitive to match the Data Cloud runtime (and config.yaml).
    session = (
        SparkSession.builder.master("local[1]")
        .appName("column-hints-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("OFF")
    install_column_casing_hints()
    yield session
    session.stop()


@pytest.fixture
def df(spark):
    # Schema is lowercase, as Data Cloud / the SDK reader would produce it.
    return spark.createDataFrame(
        [(1, 2.0, "x")], schema=["unitprice__c", "qty__c", "name__c"]
    )


# Each case references the PascalCase name "UnitPrice__c" whose lowercase form
# "unitprice__c" is a real column. All should surface the casing hint.
ACCESS_PATHS = {
    "getitem": lambda df: df["UnitPrice__c"],
    "select_str": lambda df: df.select("UnitPrice__c").collect(),
    "col_action": lambda df: df.select(col("UnitPrice__c")).collect(),
    "filter": lambda df: df.filter(col("UnitPrice__c") > 0).collect(),
    "with_column": lambda df: df.withColumn("x", col("UnitPrice__c")).collect(),
    "select_expr": lambda df: df.selectExpr("UnitPrice__c + 1").collect(),
    "getattr": lambda df: df.UnitPrice__c,
}
# Note: df.drop("UnitPrice__c") is intentionally omitted — Spark's drop()
# silently ignores non-matching names and never raises, so there is no error
# to augment (and no silent data loss risk, since nothing is dropped).


@pytest.mark.parametrize("path_name", sorted(ACCESS_PATHS))
def test_casing_hint_added_for_all_access_paths(df, path_name):
    access = ACCESS_PATHS[path_name]
    with pytest.raises(Exception) as excinfo:
        access(df)
    message = str(excinfo.value)
    assert HINT_FRAGMENT in message, f"{path_name!r} did not get a casing hint"
    assert "UnitPrice__c" in message
    assert "unitprice__c" in message


def test_original_message_preserved(df):
    """The hint is prepended; Spark's original error detail must remain."""
    with pytest.raises(Exception) as excinfo:
        df.select("UnitPrice__c").collect()
    message = str(excinfo.value)
    assert HINT_FRAGMENT in message
    # Spark's own UNRESOLVED_COLUMN text / suggestion list is still present.
    assert "UNRESOLVED_COLUMN" in message or "cannot be resolved" in message


def test_genuinely_missing_column_is_not_relabelled(df):
    """A column with no lowercase match keeps the vanilla Spark message."""
    with pytest.raises(Exception) as excinfo:
        df.select("TotallyMissing").collect()
    assert HINT_FRAGMENT not in str(excinfo.value)


def test_already_lowercase_missing_column_no_hint(df):
    """A lowercase typo is a real error, not a casing issue — no hint."""
    with pytest.raises(Exception) as excinfo:
        df.select("nonexistent__c").collect()
    assert HINT_FRAGMENT not in str(excinfo.value)


def test_getattr_missing_no_match_keeps_attribute_error(df):
    """Attribute access for a truly-absent name still raises AttributeError."""
    with pytest.raises(AttributeError):
        getattr(df, "NoSuchThing")


def test_install_is_idempotent():
    """Repeated installs must not stack wrappers."""
    from pyspark.errors.exceptions import captured
    from pyspark.sql import DataFrame

    install_column_casing_hints()
    conv1 = captured.convert_exception
    getattr1 = DataFrame.__getattr__
    install_column_casing_hints()
    assert captured.convert_exception is conv1
    assert DataFrame.__getattr__ is getattr1
