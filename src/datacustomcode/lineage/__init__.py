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
"""Generate config.json ``dataLineage`` by tracing a script's Spark plans."""

from __future__ import annotations

import builtins
import json
import os
import subprocess
import sys
import tempfile
from typing import (
    Any,
    Dict,
    NoReturn,
    Optional,
    Sequence,
)

from datacustomcode.lineage.capture import (
    LineageCaptureReader,
    LineageCaptureWriter,
    LineageCollector,
    LineageResult,
    install_capture,
)
from datacustomcode.lineage.tracer import LineageTraceError

__all__ = [
    "LineageCaptureReader",
    "LineageCaptureWriter",
    "LineageCollector",
    "LineageResult",
    "LineageTraceError",
    "generate_lineage",
    "install_capture",
]


def generate_lineage(
    entrypoint: str,
    config_file: Optional[str] = None,
    dependencies: Sequence[str] = (),
    profile: str = "default",
    sf_cli_org: Optional[str] = None,
) -> LineageResult:
    """Run a script entrypoint and return the field-level lineage it produces.

    The entrypoint runs exactly as with ``run`` (same config resolution, reader
    and credentials), except that writes are traced instead of performed. It runs
    in a fresh Python process, so no client or config from the caller's process
    leaks into the capture. The process inherits this one's working directory,
    environment, ``sys.path`` and stdout/stderr.

    Args:
        entrypoint: The script entrypoint to run.
        config_file: Optional SDK config file, as for ``run``.
        dependencies: Modules to import before running, as for ``run``.
        profile: The credentials profile to use.
        sf_cli_org: Optional SF CLI org alias or username.

    Returns:
        The captured :class:`LineageResult`.

    Raises:
        ValueError: If the package is not a batch script.
        Exception: Whatever the run raised, re-raised as the same built-in
            exception type or :class:`LineageTraceError`, else ``RuntimeError``.
    """
    with tempfile.TemporaryDirectory(prefix="datacustomcode-lineage-") as tmp:
        request_file = os.path.join(tmp, "request.json")
        response_file = os.path.join(tmp, "response.json")
        with open(request_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "args": {
                        "entrypoint": entrypoint,
                        "config_file": config_file,
                        "dependencies": list(dependencies),
                        "profile": profile,
                        "sf_cli_org": sf_cli_org,
                    },
                    "response_file": response_file,
                },
                f,
            )
        # The worker sees the same modules this process does
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
        completed = subprocess.run(
            [sys.executable, "-m", "datacustomcode.lineage._worker", request_file],
            env=env,
            check=False,
        )
        if not os.path.exists(response_file):
            raise RuntimeError(
                f"Lineage generation exited with code {completed.returncode} "
                "without a result"
            )
        with open(response_file, encoding="utf-8") as f:
            response = json.load(f)
    if "error" in response:
        _raise(response["error"])
    return LineageResult.from_dict(response["result"])


def _raise(error: Dict[str, Any]) -> NoReturn:
    """Re-raise an error reported by the worker as the closest local type."""
    message = error["message"]
    if error["type"] == LineageTraceError.__name__:
        raise LineageTraceError(message)
    exc_type = getattr(builtins, error["type"], None)
    if (
        error["module"] == "builtins"
        and isinstance(exc_type, type)
        and issubclass(exc_type, Exception)
    ):
        try:
            exc = exc_type(message)
        except TypeError:  # e.g. UnicodeDecodeError takes more arguments
            pass
        else:
            raise exc
    raise RuntimeError(f"{error['module']}.{error['type']}: {message}")
