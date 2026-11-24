from __future__ import annotations

import pytest

from datacustomcode.io.dedup_order import (
    SOURCE_RECORD_BY_LATEST_COMMIT_VERSION,
    SOURCE_RECORD_STANDARD,
    SOURCE_RECORD_UPSERT_OVER_DELETE,
    validate_dedup_order,
)

SCORE = {"column": "lead_score__c", "direction": "desc"}


def test_empty_order_is_valid():
    assert validate_dedup_order([]) == []


def test_constants_expand_to_plain_entries():
    assert SOURCE_RECORD_BY_LATEST_COMMIT_VERSION == {
        "column": "_commit_version",
        "direction": "desc",
        "partition": "_source_record_key",
    }
    assert SOURCE_RECORD_UPSERT_OVER_DELETE == {
        "column": "_merge_record_type",
        "direction": "desc",
        "partition": "_source_record_key",
    }
    assert SOURCE_RECORD_STANDARD == (
        SOURCE_RECORD_BY_LATEST_COMMIT_VERSION,
        SOURCE_RECORD_UPSERT_OVER_DELETE,
    )


def test_defaults_then_customer_entry_is_valid():
    result = validate_dedup_order([*SOURCE_RECORD_STANDARD, SCORE])

    assert result == [*SOURCE_RECORD_STANDARD, SCORE]


def test_accepts_tuple_and_returns_copies():
    result = validate_dedup_order(SOURCE_RECORD_STANDARD)

    assert result == list(SOURCE_RECORD_STANDARD)
    result[0]["direction"] = "asc"
    assert SOURCE_RECORD_BY_LATEST_COMMIT_VERSION["direction"] == "desc"


def test_partitioned_entries_may_use_any_column():
    entry = {"column": "updated_at__c", "direction": "asc", "partition": "region__c"}

    assert validate_dedup_order([entry]) == [entry]


@pytest.mark.parametrize("value", [None, "desc", b"x", {"column": "a"}, 3])
def test_non_sequence_is_rejected(value):
    with pytest.raises(ValueError, match="must be a list"):
        validate_dedup_order(value)


def test_entry_must_be_a_dict():
    with pytest.raises(ValueError, match=r"dedup_order\[1\] must be a dict"):
        validate_dedup_order([SCORE, "lead_score__c"])


@pytest.mark.parametrize("missing", ["column", "direction"])
def test_required_keys(missing):
    entry = {k: v for k, v in SCORE.items() if k != missing}

    with pytest.raises(ValueError, match=f"missing required key '{missing}'"):
        validate_dedup_order([entry])


def test_unknown_key_is_rejected():
    with pytest.raises(ValueError, match=r"unknown key\(s\) \['rank'\]"):
        validate_dedup_order([{**SCORE, "rank": ["a", "b"]}])


@pytest.mark.parametrize("direction", ["DESC", "ascending", "", None])
def test_direction_must_be_asc_or_desc(direction):
    with pytest.raises(ValueError, match="must be one of"):
        validate_dedup_order([{**SCORE, "direction": direction}])


@pytest.mark.parametrize("key", ["column", "partition"])
@pytest.mark.parametrize("value", ["", None, 1, "a.b", "a b", "1a", "x`y", "a" * 256])
def test_column_and_partition_must_be_plain_names(key, value):
    with pytest.raises(ValueError, match=f"\\['{key}'\\] must be a plain column name"):
        validate_dedup_order([{**SCORE, key: value}])


def test_partitioned_entry_after_unpartitioned_is_rejected():
    with pytest.raises(
        ValueError, match=r"dedup_order\[1\] has a 'partition' but follows"
    ):
        validate_dedup_order([SCORE, SOURCE_RECORD_BY_LATEST_COMMIT_VERSION])


def test_unpartitioned_entries_may_repeat_after_partitioned():
    result = validate_dedup_order(
        [*SOURCE_RECORD_STANDARD, SCORE, {"column": "email", "direction": "asc"}]
    )

    assert len(result) == 4
