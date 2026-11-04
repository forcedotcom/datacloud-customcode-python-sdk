import logging

from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.agentforce_scorer import (
    AgentforceScorerV1Output,
    AgentforceScorerV1Request,
    AgentforceScorerV1Response,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def function(
    request: AgentforceScorerV1Request, runtime: Runtime
) -> AgentforceScorerV1Response:
    """Score a single Agentforce session.

    Args:
        request: AgentforceScorerV1Request with the session tracing detail view
        runtime: Runtime context (unused but required by contract)

    Returns:
        AgentforceScorerV1Response with one or more open-ended results
    """
    session = request.python_scorer_input
    messages = [message for run in session.runs for message in run.messages]
    logger.info(
        f"Scoring session {session.sessionState.sessionId}: "
        f"{len(session.runs)} runs, {len(messages)} messages"
    )

    # Replace with your scoring logic
    user_turns = sum(1 for message in messages if message.actorRole == "user")
    return AgentforceScorerV1Response(
        outputs=[
            AgentforceScorerV1Output(
                label="user_turns",
                value=str(user_turns),
                is_passed=user_turns > 0,
            )
        ]
    )
