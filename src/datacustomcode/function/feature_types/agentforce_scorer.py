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
Pydantic models for Agentforce Scorer V1

The request carries the session tracing detail view of a single Agentforce
session. The response follows the open-ended scorer result contract
(``agentforce_session_tracing/scorerOpenEnded.yaml`` in Core).

Field names mirror the wire format (camelCase), so they are not renamed.
"""

from typing import (
    List,
    Optional,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

# Max length of Core "customText" fields
CUSTOM_TEXT_MAX_LENGTH = 100_000


class AgentforceScorerV1SessionState(BaseModel):
    """Session identifiers and channel information"""

    sessionId: str = Field(description="Agentforce session id")
    startTimestamp: str = Field(
        description="Session start (ISO-8601 datetime)",
        examples=["2026-01-01T10:00:00Z"],
    )
    channel: str = Field(description="Channel of the session", examples=["Messaging"])
    messagingSessionId: Optional[str] = Field(
        default=None, description="Messaging session id, if any"
    )
    voiceCallId: Optional[str] = Field(
        default=None, description="Voice call id, if any"
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Actor(BaseModel):
    """Participant in the session (end user or agent)"""

    id: str = Field(description="Participant id")
    participantObject: str = Field(description="Participant object API name")
    role: str = Field(description="Participant role", examples=["user"])
    agentApiName: Optional[str] = Field(default=None, description="Agent API name")
    agentVersion: Optional[str] = Field(default=None, description="Agent version")
    agentType: Optional[str] = Field(default=None, description="Agent type")
    agentTemplate: Optional[str] = Field(default=None, description="Agent template")
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Metrics(BaseModel):
    """Aggregate session metrics"""

    durationMs: int = Field(description="Session duration in milliseconds")
    turns: int = Field(description="Number of conversation turns")
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Message(BaseModel):
    """Single message exchanged in a run"""

    message: str = Field(description="Message text", max_length=CUSTOM_TEXT_MAX_LENGTH)
    timestamp: str = Field(
        description="Message time (ISO-8601 datetime)",
        examples=["2026-01-01T10:00:05Z"],
    )
    actorRole: str = Field(description="Role of the sender", examples=["user"])
    actorId: str = Field(description="Id of the sender")
    type: str = Field(description="Message type")
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Step(BaseModel):
    """Single step of the agent loop (classification, action, ...)

    ``input``, ``output`` and ``attributes`` are raw strings, often JSON-encoded.
    """

    stepId: str = Field(description="Step id")
    type: str = Field(description="Step type")
    name: str = Field(description="Step name")
    startTimestamp: str = Field(
        description="Step start (ISO-8601 datetime)",
        examples=["2026-01-01T10:00:06Z"],
    )
    endTimestamp: str = Field(
        description="Step end (ISO-8601 datetime)",
        examples=["2026-01-01T10:00:07Z"],
    )
    durationMs: int = Field(description="Step duration in milliseconds")
    input: Optional[str] = Field(
        default=None, description="Step input", max_length=CUSTOM_TEXT_MAX_LENGTH
    )
    output: Optional[str] = Field(
        default=None, description="Step output", max_length=CUSTOM_TEXT_MAX_LENGTH
    )
    attributes: Optional[str] = Field(
        default=None,
        description="Step attributes",
        max_length=CUSTOM_TEXT_MAX_LENGTH,
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1AgentLoop(BaseModel):
    """Agent loop steps executed within a run"""

    steps: List[AgentforceScorerV1Step] = Field(
        default_factory=list, description="Steps in execution order"
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Run(BaseModel):
    """A run (topic execution) within a session"""

    runId: str = Field(description="Run id")
    topicName: str = Field(description="Topic handled by this run")
    startTimestamp: str = Field(
        description="Run start (ISO-8601 datetime)",
        examples=["2026-01-01T10:00:00Z"],
    )
    endTimestamp: str = Field(
        description="Run end (ISO-8601 datetime)",
        examples=["2026-01-01T10:01:00Z"],
    )
    durationMs: int = Field(description="Run duration in milliseconds")
    messages: List[AgentforceScorerV1Message] = Field(
        default_factory=list, description="Messages in this run"
    )
    agentLoop: AgentforceScorerV1AgentLoop = Field(
        default_factory=AgentforceScorerV1AgentLoop,
        description="Agent loop of this run",
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1SessionTracingDetailView(BaseModel):
    """Full session tracing detail view passed to the scorer"""

    sessionState: AgentforceScorerV1SessionState = Field(
        description="Session identifiers and channel"
    )
    actors: List[AgentforceScorerV1Actor] = Field(
        default_factory=list, description="Session participants"
    )
    metrics: Optional[AgentforceScorerV1Metrics] = Field(
        default=None, description="Aggregate session metrics"
    )
    runs: List[AgentforceScorerV1Run] = Field(
        default_factory=list, description="Runs in session order"
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Output(BaseModel):
    """Single open-ended scorer result"""

    label: Optional[str] = Field(
        default=None, description="Result label", examples=["resolution"]
    )
    value: Optional[str] = Field(
        default=None, description="Result value", examples=["resolved"]
    )
    isPassed: Optional[bool] = Field(
        default=None, description="Whether the session passed this check"
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Request(BaseModel):
    """Request for Agentforce Scorer"""

    python_scorer_input: AgentforceScorerV1SessionTracingDetailView = Field(
        description="Session to score"
    )
    model_config = ConfigDict(extra="ignore")


class AgentforceScorerV1Response(BaseModel):
    """Response for Agentforce Scorer (open-ended result)"""

    outputs: List[AgentforceScorerV1Output] = Field(
        default_factory=list, description="Scorer results"
    )
    model_config = ConfigDict(extra="ignore")
