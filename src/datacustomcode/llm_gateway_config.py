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

from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
    Union,
)

from datacustomcode.common_config import BaseConfig, default_config_file
from datacustomcode.einstein_platform_config import CredentialsObjectConfig
from datacustomcode.llm_gateway.base import LLMGateway

_E = TypeVar("_E", bound=LLMGateway)


class LLMGatewayObjectConfig(CredentialsObjectConfig, Generic[_E]):
    type_to_create: ClassVar[Type[LLMGateway]] = LLMGateway  # type: ignore[type-abstract]


class LLMGatewayConfig(BaseConfig):
    llm_gateway_config: Union[LLMGatewayObjectConfig[LLMGateway], None] = None

    def update(self, other: "LLMGatewayConfig") -> "LLMGatewayConfig":
        def merge(
            config_a: Union[LLMGatewayObjectConfig, None],
            config_b: Union[LLMGatewayObjectConfig, None],
        ) -> Union[LLMGatewayObjectConfig, None]:
            if config_a is not None and config_a.force:
                return config_a
            if config_b:
                return config_b
            return config_a

        self.llm_gateway_config = merge(
            self.llm_gateway_config, other.llm_gateway_config
        )
        return self


# Global LLM Gateway config instance
llm_gateway_config = LLMGatewayConfig()
llm_gateway_config.load(default_config_file())
