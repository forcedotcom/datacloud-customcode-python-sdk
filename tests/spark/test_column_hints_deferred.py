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
"""Tests for deferred (pyspark-free) installation of the column-casing hooks.

These exercise ``install_column_casing_hints`` before ``pyspark.sql`` has
been imported. ``test_column_hints.py`` always imports ``pyspark.sql`` at
module scope, so it can never observe the deferred branch; the subprocess
tests here start from a clean interpreter where pyspark genuinely hasn't
been imported yet.
"""
from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from datacustomcode.spark import column_hints
from datacustomcode.spark.column_hints import _PySparkImportHook


def _run(script: str) -> None:
    """Run *script* in a fresh interpreter; fail loudly on error."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            f"subprocess failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def test_install_before_pyspark_import_does_not_import_pyspark() -> None:
    _run(
        """
        import sys
        from datacustomcode.spark.column_hints import install_column_casing_hints

        assert "pyspark" not in sys.modules
        install_column_casing_hints()
        assert "pyspark" not in sys.modules
        assert "pyspark.sql" not in sys.modules
        """
    )


def test_install_before_pyspark_import_registers_finder() -> None:
    _run(
        """
        import sys
        from datacustomcode.spark.column_hints import (
            _PySparkImportHook,
            install_column_casing_hints,
        )

        install_column_casing_hints()
        hooks = [f for f in sys.meta_path if isinstance(f, _PySparkImportHook)]
        assert len(hooks) == 1
        """
    )


def test_repeated_deferred_install_does_not_stack_finders() -> None:
    _run(
        """
        import sys
        from datacustomcode.spark.column_hints import (
            _PySparkImportHook,
            install_column_casing_hints,
        )

        install_column_casing_hints()
        install_column_casing_hints()
        install_column_casing_hints()
        hooks = [f for f in sys.meta_path if isinstance(f, _PySparkImportHook)]
        assert len(hooks) == 1
        """
    )


@pytest.mark.spark
def test_hooks_install_when_pyspark_sql_is_later_imported() -> None:
    _run(
        """
        from datacustomcode.spark.column_hints import (
            _WRAPPED_MARKER,
            install_column_casing_hints,
        )

        install_column_casing_hints()

        import pyspark.sql  # noqa: F401 - triggers the deferred install
        from pyspark.errors.exceptions import captured
        from pyspark.sql import DataFrame

        assert getattr(captured.convert_exception, _WRAPPED_MARKER, False)
        assert getattr(DataFrame.__getattr__, _WRAPPED_MARKER, False)
        """
    )


@pytest.mark.spark
def test_install_installs_immediately_when_pyspark_already_loaded() -> None:
    _run(
        """
        import sys

        import pyspark.sql  # noqa: F401 - pyspark is loaded before install
        from datacustomcode.spark.column_hints import (
            _PySparkImportHook,
            _WRAPPED_MARKER,
            install_column_casing_hints,
        )

        install_column_casing_hints()

        from pyspark.errors.exceptions import captured
        from pyspark.sql import DataFrame

        assert getattr(captured.convert_exception, _WRAPPED_MARKER, False)
        assert getattr(DataFrame.__getattr__, _WRAPPED_MARKER, False)
        assert not any(isinstance(f, _PySparkImportHook) for f in sys.meta_path)
        """
    )


class _FakeLoader:
    def __init__(self) -> None:
        self.exec_calls: list[object] = []

    def exec_module(self, module: object) -> None:
        self.exec_calls.append(module)


class _FakeSpec:
    def __init__(self, loader: _FakeLoader) -> None:
        self.loader = loader


class _FakeFinder:
    """Stands in for the real pyspark finder on ``sys.meta_path``."""

    def __init__(self, spec: object | None) -> None:
        self._spec = spec

    def find_spec(self, fullname, path, target=None):  # type: ignore[no-untyped-def]
        return self._spec


def test_find_spec_ignores_other_module_names() -> None:
    hook = _PySparkImportHook()
    assert hook.find_spec("pyspark", None) is None
    assert hook.find_spec("some.other.module", None) is None


def test_find_spec_returns_none_when_no_other_finder_can_resolve(monkeypatch) -> None:
    hook = _PySparkImportHook()
    monkeypatch.setattr(sys, "meta_path", [hook, _FakeFinder(None)])
    assert hook.find_spec("pyspark.sql", None) is None


def test_find_spec_wraps_loader_exec_module_and_installs_hooks_after(
    monkeypatch,
) -> None:
    loader = _FakeLoader()
    spec = _FakeSpec(loader)
    hook = _PySparkImportHook()
    monkeypatch.setattr(sys, "meta_path", [hook, _FakeFinder(spec)])

    calls: list[str] = []
    monkeypatch.setattr(
        column_hints, "_install_hooks_now", lambda: calls.append("installed")
    )

    found = hook.find_spec("pyspark.sql", None)
    assert found is spec
    # The loader's exec_module has been swapped for a wrapper.
    assert loader.exec_module is not _FakeLoader.exec_module.__get__(loader)

    fake_module = object()
    spec.loader.exec_module(fake_module)
    assert loader.exec_calls == [fake_module]  # original behavior preserved
    assert calls == ["installed"]  # hint hooks installed right after
