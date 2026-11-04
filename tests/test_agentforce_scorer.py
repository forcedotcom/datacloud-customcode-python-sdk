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

"""Tests for the AgentforceScorer function feature."""

import json
import os
from unittest.mock import patch

from click.testing import CliRunner

from datacustomcode.cli import deploy, init
from datacustomcode.deploy import AccessTokenResponse, infer_use_in_feature
from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.agentforce_scorer import (
    AgentforceScorerV1Request,
    AgentforceScorerV1Response,
)
from datacustomcode.function_utils import (
    generate_test_json,
    get_function_callable,
    load_function_module,
)
from datacustomcode.scan import write_sdk_config
from datacustomcode.template import function_template_dir

SAMPLE_REQUEST = {
    "python_scorer_input": {
        "sessionState": {
            "sessionId": "0Xx000000000001",
            "startTimestamp": "2026-01-01T10:00:00Z",
            "channel": "Messaging",
        },
        "actors": [
            {"id": "u1", "participantObject": "Contact", "role": "user"},
            {
                "id": "a1",
                "participantObject": "Bot",
                "role": "agent",
                "agentApiName": "Service_Agent",
            },
        ],
        "metrics": {"durationMs": 60000, "turns": 2},
        "runs": [
            {
                "runId": "r1",
                "topicName": "Order_Status",
                "startTimestamp": "2026-01-01T10:00:00Z",
                "endTimestamp": "2026-01-01T10:01:00Z",
                "durationMs": 60000,
                "messages": [
                    {
                        "message": "Where is my order?",
                        "timestamp": "2026-01-01T10:00:05Z",
                        "actorRole": "user",
                        "actorId": "u1",
                        "type": "Text",
                    }
                ],
                "agentLoop": {
                    "steps": [
                        {
                            "stepId": "s1",
                            "type": "Action",
                            "name": "GetOrder",
                            "startTimestamp": "2026-01-01T10:00:06Z",
                            "endTimestamp": "2026-01-01T10:00:07Z",
                            "durationMs": 1000,
                            "input": '{"orderId": "1"}',
                        }
                    ]
                },
                "unknownField": "ignored",
            }
        ],
    }
}


def test_request_parses_session():
    request = AgentforceScorerV1Request(**SAMPLE_REQUEST)
    session = request.python_scorer_input
    assert session.sessionState.sessionId == "0Xx000000000001"
    assert session.sessionState.voiceCallId is None
    assert session.actors[1].agentApiName == "Service_Agent"
    assert session.runs[0].agentLoop.steps[0].output is None
    assert session.runs[0].messages[0].actorRole == "user"


def test_run_agent_loop_defaults_to_empty():
    run = dict(SAMPLE_REQUEST["python_scorer_input"]["runs"][0])
    run.pop("agentLoop")
    payload = {
        "python_scorer_input": {
            **SAMPLE_REQUEST["python_scorer_input"],
            "runs": [run],
        }
    }
    request = AgentforceScorerV1Request(**payload)
    assert request.python_scorer_input.runs[0].agentLoop.steps == []


def test_response_serializes_outputs():
    response = AgentforceScorerV1Response(
        outputs=[{"label": "resolution", "value": "resolved", "isPassed": True}]
    )
    assert response.model_dump() == {
        "outputs": [{"label": "resolution", "value": "resolved", "isPassed": True}]
    }
    assert AgentforceScorerV1Response().model_dump() == {"outputs": []}


def test_init_scaffolds_agentforce_scorer_template():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            init,
            [
                "proj",
                "--code-type",
                "function",
                "--use-in-feature",
                "AgentforceScorer",
            ],
        )
        assert result.exit_code == 0, result.output

        entrypoint = os.path.join("proj", "payload", "entrypoint.py")
        with open(entrypoint) as f:
            content = f.read()
        assert "AgentforceScorerV1Request" in content
        assert "SearchIndexChunkingV1Request" not in content
        assert infer_use_in_feature(entrypoint) == "AgentforceScorer"

        test_json = os.path.join("proj", "payload", "tests", "test.json")
        with open(test_json) as f:
            AgentforceScorerV1Request(**json.load(f))


def test_init_rejects_unknown_function_feature():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            init,
            ["proj", "--code-type", "function", "--use-in-feature", "NotAFeature"],
        )
        assert result.exit_code != 0
        assert "Invalid --use-in-feature" in result.output


def test_template_entrypoint_runs(tmp_path):
    entrypoint = os.path.join(
        function_template_dir, "agentforce_scorer", "payload", "entrypoint.py"
    )
    module = load_function_module(entrypoint, "scorer_entrypoint")
    result = get_function_callable(module)(
        AgentforceScorerV1Request(**SAMPLE_REQUEST), Runtime()
    )
    assert isinstance(result, AgentforceScorerV1Response)
    assert result.outputs[0].label == "user_turns"
    assert result.outputs[0].value == "1"
    assert result.outputs[0].isPassed is True

    output = tmp_path / "test.json"
    generate_test_json(entrypoint, str(output))
    AgentforceScorerV1Request(**json.loads(output.read_text()))


@patch("datacustomcode.deploy.infer_use_in_feature")
@patch("datacustomcode.deploy.deploy_full")
@patch("datacustomcode.token_provider.CredentialsTokenProvider")
def test_deploy_sends_agentforce_scorer_via_invoke_options(
    mock_token_provider, mock_deploy_full, mock_infer_feature
):
    mock_token_provider.return_value.get_token.return_value = AccessTokenResponse(
        access_token="t", instance_url="https://instance.example.com"
    )
    mock_infer_feature.return_value = "AgentforceScorer"

    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("payload", exist_ok=True)
        write_sdk_config(".", {"type": "function"})
        result = runner.invoke(deploy, ["--name", "scorer"])

        assert result.exit_code == 0, result.output
        metadata = mock_deploy_full.call_args[0][1]
        assert metadata.invokeOptions == ["AgentforceScorer"]
        assert metadata.functionInvokeOptions is None


@patch("datacustomcode.deploy.infer_use_in_feature")
@patch("datacustomcode.deploy.deploy_full")
@patch("datacustomcode.token_provider.CredentialsTokenProvider")
def test_deploy_keeps_chunking_on_legacy_field(
    mock_token_provider, mock_deploy_full, mock_infer_feature
):
    mock_token_provider.return_value.get_token.return_value = AccessTokenResponse(
        access_token="t", instance_url="https://instance.example.com"
    )
    mock_infer_feature.return_value = "SearchIndexChunking"

    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("payload", exist_ok=True)
        write_sdk_config(".", {"type": "function"})
        result = runner.invoke(deploy, ["--name", "chunker"])

        assert result.exit_code == 0, result.output
        metadata = mock_deploy_full.call_args[0][1]
        assert metadata.functionInvokeOptions == ["UnstructuredChunking"]
        assert metadata.invokeOptions is None
