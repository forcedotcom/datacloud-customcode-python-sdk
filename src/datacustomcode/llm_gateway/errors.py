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
"""Exceptions raised by LLM Gateway implementations."""

from __future__ import annotations

from typing import Optional


class LLMGatewayCallError(RuntimeError):
    """Raised when an LLM Gateway call returns an error."""

    def __init__(
        self,
        message: str,
        *,
        status: Optional[object] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.error_code = error_code
        self.error_message = error_message
