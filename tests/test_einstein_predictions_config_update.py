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

import tempfile
import os
import yaml

from datacustomcode.einstein_predictions_config import EinsteinPredictionsConfig, EinsteinPredictionsObjectConfig
from datacustomcode.einstein_predictions.impl.default import DefaultEinsteinPredictions


class TestEinsteinPredictionsConfigUpdate:
    def test_update_replaces_config_without_force(self):
        config1 = EinsteinPredictionsConfig(
            einstein_predictions_config=EinsteinPredictionsObjectConfig(
                type_config_name="OldImplementation",
                options={"old": True}
            )
        )

        config2 = EinsteinPredictionsConfig(
            einstein_predictions_config=EinsteinPredictionsObjectConfig(
                type_config_name="NewImplementation",
                options={"new": True}
            )
        )

        config1.update(config2)

        assert config1.einstein_predictions_config.type_config_name == "NewImplementation"
        assert config1.einstein_predictions_config.options == {"new": True}

    def test_update_respects_force_flag(self):
        config1 = EinsteinPredictionsConfig(
            einstein_predictions_config=EinsteinPredictionsObjectConfig(
                type_config_name="ForcedImplementation",
                options={"forced": True},
                force=True
            )
        )

        config2 = EinsteinPredictionsConfig(
            einstein_predictions_config=EinsteinPredictionsObjectConfig(
                type_config_name="NewImplementation",
                options={"new": True}
            )
        )

        config1.update(config2)

        assert config1.einstein_predictions_config.type_config_name == "ForcedImplementation"
        assert config1.einstein_predictions_config.options == {"forced": True}
        assert config1.einstein_predictions_config.force is True


class TestEinsteinPredictionsConfigLoad:
    def test_load_from_yaml_file(self):
        config_data = {
            "einstein_predictions_config": {
                "type_config_name": "DefaultEinsteinPredictions"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name

        try:
            config = EinsteinPredictionsConfig()
            config.load(temp_file)

            assert config.einstein_predictions_config is not None
            assert config.einstein_predictions_config.type_config_name == "DefaultEinsteinPredictions"
            einstein_predictions = config.einstein_predictions_config.to_object()
            assert einstein_predictions is not None
            assert isinstance(einstein_predictions, DefaultEinsteinPredictions)
        finally:
            os.unlink(temp_file)

