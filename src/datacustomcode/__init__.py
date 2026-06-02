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

__all__ = [
    "AuthType",
    "Client",
    "Credentials",
    "DefaultSparkLLMGateway",
    "PrintDataCloudWriter",
    "QueryAPIDataCloudReader",
    "SparkLLMGateway",
    "llm_gateway_generate_text_col",
]


def __getattr__(name: str):
    """Lazy import heavy dependencies."""
    if name == "Client":
        from datacustomcode.client import Client

        return Client
    elif name == "AuthType":
        from datacustomcode.credentials import AuthType

        return AuthType
    elif name == "Credentials":
        from datacustomcode.credentials import Credentials

        return Credentials
    elif name == "PrintDataCloudWriter":
        from datacustomcode.io.writer.print import PrintDataCloudWriter

        return PrintDataCloudWriter
    elif name == "QueryAPIDataCloudReader":
        from datacustomcode.io.reader.query_api import QueryAPIDataCloudReader

        return QueryAPIDataCloudReader
    elif name == "SparkLLMGateway":
        from datacustomcode.llm_gateway import SparkLLMGateway

        return SparkLLMGateway
    elif name == "DefaultSparkLLMGateway":
        from datacustomcode.llm_gateway import DefaultSparkLLMGateway

        return DefaultSparkLLMGateway
    elif name == "llm_gateway_generate_text_col":
        from datacustomcode.client import llm_gateway_generate_text_col

        return llm_gateway_generate_text_col
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
