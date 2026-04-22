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


import threading
from typing import Optional

from datacustomcode.file.path.default import DefaultFindFilePath
from datacustomcode.function.base import BaseRuntime
from datacustomcode.llm_gateway.default import DefaultLLMGateway


class Runtime(BaseRuntime):
    """Client for Function code type.

    NOTE: Do not instantiate this class directly.
    It will be provided to your function by the SDK:

        def function(request: dict, runtime: RunTime) -> dict:
            response = {...}
            return response

    """

    _instance: Optional["Runtime"] = None
    _lock = threading.Lock()

    def __new__(cls):
        """Create singleton instance (thread-safe)."""
        with cls._lock:
            if cls._instance is not None:
                raise RuntimeError(
                    "Runtime can only be instantiated once by the SDK.\n\n"
                    "Do not instantiate it yourself. Accept it as a parameter:\n\n"
                    "  from datacustomcode.runtime.function.RunTime import Function\n"
                    "  \n"
                    "  def function(request: dict, runtime: Runtime) -> dict:\n"
                    "      response = {...}\n"
                    "      return response"
                )
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Prevent re-initialization
        if hasattr(self, "_initialized"):
            return

        self._initialized = True

        super().__init__()

        # Initialize resources
        self._llm_gateway = DefaultLLMGateway()
        self._file = DefaultFindFilePath()

    @property
    def llm_gateway(self) -> DefaultLLMGateway:
        """Access LLM operations."""
        return self._llm_gateway

    @property
    def file(self) -> DefaultFindFilePath:
        """Access file operations."""
        return self._file
