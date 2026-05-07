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

from typing import Any, Callable, Type, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def requestSchema(cls: Type[Any]) -> Callable[[F], F]:
    """Decorator that declares the request body schema for an ``@entry_func`` function.

    The *cls* argument must be a class with typed fields (e.g. a
    ``TypedDict`` subclass).  At runtime the decorator is a pass-through —
    it returns the original function unchanged.  The schema generator
    reads the decorator at the AST level to produce OpenAPI ``properties``.

    Example::

        from typing import TypedDict
        from datacustomcode.schema import requestSchema

        class AddRequest(TypedDict):
            a: int
            b: int

        @entry_func
        @requestSchema(AddRequest)
        def add(request: dict) -> dict:
            ...
    """
    def decorator(fn: F) -> F:
        return fn
    return decorator


def responseSchema(cls: Type[Any]) -> Callable[[F], F]:
    """Decorator that declares the response body schema for an ``@entry_func`` function.

    The *cls* argument must be a class with typed fields (e.g. a
    ``TypedDict`` subclass).  At runtime the decorator is a pass-through —
    it returns the original function unchanged.  The schema generator
    reads the decorator at the AST level to produce OpenAPI ``properties``.

    Example::

        from typing import TypedDict
        from datacustomcode.schema import responseSchema

        class AddResponse(TypedDict):
            result: int

        @entry_func
        @responseSchema(AddResponse)
        def add(request: dict) -> dict:
            ...
    """
    def decorator(fn: F) -> F:
        return fn
    return decorator
