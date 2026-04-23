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

import os
import tempfile

import yaml

from datacustomcode.llm_gateway.default import DefaultLLMGateway
from datacustomcode.llm_gateway_config import (
    LLMGatewayConfig,
    LLMGatewayObjectConfig,
)


class TestLLMGatewayConfigUpdate:
    def test_update_replaces_config_without_force(self):
        config1 = LLMGatewayConfig(
            llm_gateway_config=LLMGatewayObjectConfig(
                type_config_name="OldImplementation", options={"old": True}
            )
        )

        config2 = LLMGatewayConfig(
            llm_gateway_config=LLMGatewayObjectConfig(
                type_config_name="NewImplementation", options={"new": True}
            )
        )

        config1.update(config2)

        assert config1.llm_gateway_config.type_config_name == "NewImplementation"
        assert config1.llm_gateway_config.options == {"new": True}

    def test_update_respects_force_flag(self):
        config1 = LLMGatewayConfig(
            llm_gateway_config=LLMGatewayObjectConfig(
                type_config_name="ForcedImplementation",
                options={"forced": True},
                force=True,
            )
        )

        config2 = LLMGatewayConfig(
            llm_gateway_config=LLMGatewayObjectConfig(
                type_config_name="NewImplementation", options={"new": True}
            )
        )

        config1.update(config2)

        assert (
            config1.llm_gateway_config.type_config_name == "ForcedImplementation"
        )
        assert config1.llm_gateway_config.options == {"forced": True}
        assert config1.llm_gateway_config.force is True


class TestLLMGatewayConfigLoad:
    def test_load_from_yaml_file(self):
        config_data = {
            "llm_gateway_config": {"type_config_name": "DefaultLLMGateway"}
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name

        try:
            config = LLMGatewayConfig()
            config.load(temp_file)

            assert config.llm_gateway_config is not None
            assert (
                config.llm_gateway_config.type_config_name == "DefaultLLMGateway"
            )
            llm_gateway = config.llm_gateway_config.to_object()
            assert llm_gateway is not None
            assert isinstance(llm_gateway, DefaultLLMGateway)
        finally:
            os.unlink(temp_file)
