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
"""``dedup_order`` entries for the streaming ``write_*_deltas`` methods.

Each entry is ``{"column", "direction", "partition"}``. Entries run in order and
each narrows, within its partition, to the rows tied for best on its column. An
entry without ``partition`` runs per target primary key. Entries with
``partition`` must come before entries without it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Final

from datacustomcode.io import cdf

DIRECTIONS: Final = ("asc", "desc")

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,254}")

_ENTRY_KEYS: Final = frozenset({"column", "direction", "partition"})

DedupOrderEntry = dict[str, str]

SOURCE_RECORD_BY_LATEST_COMMIT_VERSION: Final[DedupOrderEntry] = {
    "column": cdf.COMMIT_VERSION,
    "direction": "desc",
    "partition": cdf.SOURCE_RECORD_KEY,
}

# "UPSERT" sorts after "DELETE", so descending puts UPSERT first.
SOURCE_RECORD_UPSERT_OVER_DELETE: Final[DedupOrderEntry] = {
    "column": cdf.MERGE_RECORD_TYPE,
    "direction": "desc",
    "partition": cdf.SOURCE_RECORD_KEY,
}

SOURCE_RECORD_STANDARD: Final = (
    SOURCE_RECORD_BY_LATEST_COMMIT_VERSION,
    SOURCE_RECORD_UPSERT_OVER_DELETE,
)


def _validate_entry(index: int, entry: object) -> DedupOrderEntry:
    where = f"dedup_order[{index}]"
    if not isinstance(entry, Mapping):
        raise ValueError(f"{where} must be a dict, got {type(entry).__name__}.")

    unknown = sorted(str(key) for key in entry.keys() - _ENTRY_KEYS)
    if unknown:
        raise ValueError(
            f"{where} has unknown key(s) {unknown}; "
            "entries take 'column', 'direction' and optional 'partition'."
        )

    for key in ("column", "direction"):
        if key not in entry:
            raise ValueError(f"{where} is missing required key '{key}'.")

    for key in ("column", "partition"):
        if key in entry and not (
            isinstance(entry[key], str) and _NAME.fullmatch(entry[key])
        ):
            raise ValueError(
                f"{where}['{key}'] must be a plain column name: letters, digits "
                "and underscores, not starting with a digit."
            )

    if entry["direction"] not in DIRECTIONS:
        raise ValueError(
            f"{where}['direction'] must be one of {list(DIRECTIONS)}, "
            f"got {entry['direction']!r}."
        )

    return dict(entry)


def validate_dedup_order(
    dedup_order: Sequence[Mapping[str, str]],
) -> list[DedupOrderEntry]:
    """Check the shape of ``dedup_order`` and return it as a list of plain dicts.

    Raises:
        ValueError: If ``dedup_order`` or any of its entries is malformed.
    """
    if isinstance(dedup_order, (str, bytes, Mapping)) or not isinstance(
        dedup_order, Sequence
    ):
        raise ValueError(
            "dedup_order must be a list of entries; pass [] to fail the batch "
            f"when two different rows share a key. Got {type(dedup_order).__name__}."
        )

    entries = [_validate_entry(i, entry) for i, entry in enumerate(dedup_order)]

    first_unpartitioned = next(
        (i for i, entry in enumerate(entries) if "partition" not in entry), None
    )
    if first_unpartitioned is not None:
        late = next(
            (
                i
                for i, entry in enumerate(entries)
                if i > first_unpartitioned and "partition" in entry
            ),
            None,
        )
        if late is not None:
            raise ValueError(
                f"dedup_order[{late}] has a 'partition' but follows "
                f"dedup_order[{first_unpartitioned}], which has none. "
                "Entries with 'partition' must come before entries without."
            )

    return entries
