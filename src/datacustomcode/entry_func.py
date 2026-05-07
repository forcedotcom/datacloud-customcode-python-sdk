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

from __future__ import annotations

import functools
from typing import (
    Any,
    Callable,
    TypeVar,
)

F = TypeVar("F", bound=Callable[..., Any])


def entry_func(fn: F) -> F:
    """Mark a function as the BYOC entry point.

    The ``@entry_func`` decorator identifies a function whose signature
    will be extracted by ``generate_byoc_schema.py`` to produce an
    OpenAPI specification.  At runtime the decorator is a pass-through —
    it returns the original function unchanged.

    All parameters of the decorated function **must** have type
    annotations; the schema generator will reject untyped parameters.

    Example::

        from datacustomcode.entry_func import entry_func

        @entry_func
        def add(a: int, b: int = 0) -> int:
            return a + b
    """
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
