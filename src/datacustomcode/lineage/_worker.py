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
"""Runs one lineage capture in a fresh process, for ``generate_lineage``.

Usage: ``python -m datacustomcode.lineage._worker <request.json>``. The request
holds the ``run_capture`` arguments and the path of a response file, which gets
either ``{"result": LineageResult.to_dict()}`` or
``{"error": {"module", "type", "message"}}``. The response goes to a file rather
than stdout because the entrypoint prints freely to stdout.
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import (
    List,
    Optional,
    Sequence,
)

from datacustomcode.lineage.capture import (
    LineageCollector,
    LineageResult,
    install_capture,
)


def run_capture(
    entrypoint: str,
    config_file: Optional[str],
    dependencies: Sequence[str],
    profile: str,
    sf_cli_org: Optional[str],
) -> LineageResult:
    """Run the entrypoint with capture installed. Call only in a fresh process."""
    from datacustomcode.run import (
        execute_entrypoint,
        import_dependencies,
        prepare_run,
    )

    # Dependencies are imported after capture is installed, so a Client they
    # create at import time is captured too
    context = prepare_run(entrypoint, config_file, [], profile, sf_cli_org)
    if context.package_type != "script":
        raise ValueError(
            f"Lineage can only be generated for script packages, not "
            f"{context.package_type} packages"
        )
    if context.config_json.get("streamingSource"):
        raise ValueError("Lineage generation does not support streaming scripts")

    collector = LineageCollector()
    install_capture(collector)
    import_dependencies(list(dependencies))
    execute_entrypoint(entrypoint, context)
    return collector.result()


def main(argv: List[str]) -> int:
    with open(argv[0], encoding="utf-8") as f:
        request = json.load(f)
    try:
        result = run_capture(**request["args"])
        response: dict = {"result": result.to_dict()}
    except Exception as exc:
        # The caller re-raises from the response; the traceback is for the
        # customer, pointing into their entrypoint
        traceback.print_exc()
        response = {
            "error": {
                "module": type(exc).__module__,
                "type": type(exc).__name__,
                "message": str(exc),
            }
        }
    with open(request["response_file"], "w", encoding="utf-8") as f:
        json.dump(response, f)
    return 1 if "error" in response else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
