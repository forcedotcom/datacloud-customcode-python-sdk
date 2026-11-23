import logging

from datacustomcode.function import Runtime
from datacustomcode.function.feature_types.knowledge_harmonization_post_processing import (
    KnowledgeHarmonizationPostProcessingV1Output,
    KnowledgeHarmonizationPostProcessingV1Request,
    KnowledgeHarmonizationPostProcessingV1Response,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Injected banner. Inline styles because the harmonized HTML is consumed standalone.
BANNER = (
    '<section style="font-size:48px;font-weight:bold;text-align:center;'
    'padding:24px;border:4px solid #000;">BYOC APPLIED</section>'
)


def apply_banner(html: str) -> str:
    """Return the HTML with the BYOC APPLIED banner inserted at the top of the body."""
    lower = html.lower()
    body_open = lower.find("<body")
    if body_open != -1:
        insert_at = html.find(">", body_open) + 1
        return html[:insert_at] + BANNER + html[insert_at:]
    return BANNER + html


def function(
    request: KnowledgeHarmonizationPostProcessingV1Request, runtime: Runtime
) -> KnowledgeHarmonizationPostProcessingV1Response:
    """Insert a banner after <body> in the single harmonized document.

    Args:
        request: One harmonized content element in request.input.text.
        runtime: Runtime instance supplied by the customer-function host.

    Returns:
        The same document with the banner, in response.output.text.
    """
    logger.info("transform_html: received 1 document(s)")
    logger.info("Runtime available: %s", runtime is not None)
    return KnowledgeHarmonizationPostProcessingV1Response(
        output=KnowledgeHarmonizationPostProcessingV1Output(
            text=apply_banner(request.input.text)
        )
    )
