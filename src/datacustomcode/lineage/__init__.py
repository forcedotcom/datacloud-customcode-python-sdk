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

from typing import Optional, Sequence

from datacustomcode.lineage.capture import (
    LineageCaptureReader,
    LineageCaptureWriter,
    LineageCollector,
    LineageResult,
    capture_lineage,
)
from datacustomcode.lineage.tracer import LineageTraceError

__all__ = [
    "LineageCaptureReader",
    "LineageCaptureWriter",
    "LineageCollector",
    "LineageResult",
    "LineageTraceError",
    "capture_lineage",
    "generate_lineage",
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
    and credentials), except that writes are traced instead of performed.

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
    """
    from datacustomcode.run import (
        execute_entrypoint,
        import_dependencies,
        prepare_run,
    )

    # Dependencies are imported inside the capture, so a Client they create at
    # import time is captured too
    context = prepare_run(entrypoint, config_file, [], profile, sf_cli_org)
    if context.package_type != "script":
        raise ValueError(
            f"Lineage can only be generated for script packages, not "
            f"{context.package_type} packages"
        )
    if context.config_json.get("streamingSource"):
        raise ValueError("Lineage generation does not support streaming scripts")

    collector = LineageCollector()
    with capture_lineage(collector):
        import_dependencies(list(dependencies))
        execute_entrypoint(entrypoint, context)
    return collector.result()
