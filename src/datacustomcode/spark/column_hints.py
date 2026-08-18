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
"""Friendlier errors when a script references a column by the wrong case.

Data Cloud column names are lowercase. A reference using another case (e.g.
``df["UnitPrice__c"]``) fails to resolve with a generic error. These hooks
detect when the lowercase form of a referenced name is a real column and
prepend an explicit hint, keeping the original message:

    Column 'UnitPrice__c' not found. Did you mean 'unitprice__c'?
    Data Cloud columns must be lowercase.

Two hooks cover all access paths: one for column-resolution errors, and one
for attribute access (``df.X``), which fails separately. Both only augment an
error that was already going to be raised, and only for a pure casing mismatch.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Marker on our wrappers so repeated installs never stack.
_WRAPPED_MARKER = "_datacustomcode_wrapped"


def _strip_backticks(name: str) -> str:
    """Return the bare column name from a Spark identifier token."""
    name = name.strip()
    if "." in name:  # drop any table/alias qualifier
        name = name.split(".")[-1]
    return name.strip().strip("`")


def _casing_hint(bad: str, suggestion: str) -> str:
    return (
        f"Column '{bad}' not found. Did you mean '{suggestion}'? "
        "Data Cloud columns must be lowercase."
    )


def _lowercase_match(bad: str, available: list[str]) -> str | None:
    """Return the column matching *bad* case-insensitively, else ``None``.

    Returns ``None`` when *bad* is already lowercase, so a real typo is not
    reported as a casing issue.
    """
    if not bad or bad == bad.lower():
        return None
    lowered = bad.lower()
    for col in available:
        if col.lower() == lowered:
            return col
    return None


def _install_analysis_exception_hook() -> None:
    """Add a casing hint to column-resolution errors."""
    from pyspark.errors.exceptions import captured

    original = captured.convert_exception
    if getattr(original, _WRAPPED_MARKER, False):
        return

    def convert_exception_with_hint(e):  # type: ignore[no-untyped-def]
        exc = original(e)
        try:
            error_class = exc.getErrorClass()
        except Exception:  # pragma: no cover - defensive
            return exc
        if not error_class or not error_class.startswith("UNRESOLVED_COLUMN"):
            return exc

        params = exc.getMessageParameters() or {}
        bad = _strip_backticks(params.get("objectName", ""))
        proposal = params.get("proposal", "")
        available = [
            _strip_backticks(tok) for tok in proposal.split(",") if tok.strip()
        ]

        suggestion = _lowercase_match(bad, available)
        if suggestion is not None:  # prepend hint; keep original detail
            exc.desc = f"{_casing_hint(bad, suggestion)}\n{exc.desc}"
        return exc

    setattr(convert_exception_with_hint, _WRAPPED_MARKER, True)
    captured.convert_exception = convert_exception_with_hint


def _install_getattr_hook() -> None:
    """Add a casing hint to attribute access (``df.X``)."""
    from pyspark.sql import DataFrame

    original = DataFrame.__getattr__
    if getattr(original, _WRAPPED_MARKER, False):
        return

    def __getattr__with_hint(self, name):  # type: ignore[no-untyped-def]
        try:
            return original(self, name)
        except AttributeError as exc:
            if name.startswith("_"):  # leave dunder/private lookups alone
                raise
            suggestion = _lowercase_match(name, list(self.columns))
            if suggestion is not None:  # prepend hint; keep original text
                raise AttributeError(
                    f"{_casing_hint(name, suggestion)}\n{exc}"
                ) from None
            raise

    setattr(__getattr__with_hint, _WRAPPED_MARKER, True)
    DataFrame.__getattr__ = __getattr__with_hint  # type: ignore[assignment]


def install_column_casing_hints() -> None:
    """Install the column-casing hint hooks. Idempotent; never raises."""
    try:
        _install_analysis_exception_hook()
        _install_getattr_hook()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(f"Could not install column-casing hint hooks: {exc}")
