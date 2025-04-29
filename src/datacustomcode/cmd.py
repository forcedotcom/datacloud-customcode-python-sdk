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
"""
This module is shamelessly copied from conda to nicely wrap subprocess calls.
"""

from __future__ import annotations

import contextlib
import subprocess
from typing import Any, Union


def _force_bytes(exc: Any) -> bytes:
    with contextlib.suppress(TypeError):
        return bytes(exc)
    with contextlib.suppress(Exception):
        return str(exc).encode()
    return f"<unprintable {type(exc).__name__} object>".encode()


def _setdefault_kwargs(kwargs: dict[str, Any]) -> None:
    for arg in ("stdin", "stdout", "stderr"):
        kwargs.setdefault(arg, subprocess.PIPE)


def _oserror_to_output(e: OSError) -> tuple[int, bytes, None]:
    return 1, _force_bytes(e).rstrip(b"\n") + b"\n", None


class CalledProcessError(RuntimeError):
    """Nicely formatted subprocess call error."""

    def __init__(
        self,
        returncode: int,
        cmd: tuple[str, ...],
        stdout: bytes,
        stderr: Union[bytes, None],
    ) -> None:
        super().__init__(returncode, cmd, stdout, stderr)
        self.returncode = returncode
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr

    def __bytes__(self) -> bytes:
        def _indent_or_none(part: Union[bytes, None]) -> bytes:
            if part:
                return b"\n    " + part.replace(b"\n", b"\n    ").rstrip()
            else:
                return b" (none)"

        return b"".join(
            (
                f"command: {self.cmd!r}\n".encode(),
                f"return code: {self.returncode}\n".encode(),
                b"stdout:",
                self.stdout,
                b"\n",
                b"stderr:",
                _indent_or_none(self.stderr),
            )
        )

    def __str__(self) -> str:
        return self.__bytes__().decode()


def _cmd_output(
    *cmd: str,
    check: bool = True,
    **kwargs: Any,
) -> tuple[int, bytes, Union[bytes, None]]:
    _setdefault_kwargs(kwargs)
    try:
        kwargs.setdefault("shell", True)
        proc = subprocess.Popen(cmd, **kwargs)
    except OSError as e:
        returncode, stdout_b, stderr_b = _oserror_to_output(e)
    else:
        stdout_b, stderr_b = proc.communicate()
        returncode = proc.returncode
    if check and returncode:
        raise CalledProcessError(returncode, cmd, stdout_b, stderr_b)

    return returncode, stdout_b, stderr_b


def cmd_output(*cmd: str, **kwargs: Any) -> Union[str, None]:
    returncode, stdout_b, stderr_b = _cmd_output(*cmd, **kwargs)
    stdout = stdout_b.decode() if stdout_b is not None else None
    return stdout
