#!/usr/bin/env python3
# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2

"""
Data Transform with a Gemini Named Credential Callout

This is a batch transform: read a DLO, enrich it via Google's Gemini ``generateContent``
API, and write the result back to a DLO.

It shows **both** callout paths against the same Named Credential
(``callout:gemini``); the request template (URL, method, headers) is shared and
only the body differs:

- **Driver path** — :meth:`Client.named_credential_request` runs **once** on the
  driver and returns an :class:`HTTPResponse`. Use it for a single job-level
  callout whose result you reuse across the job.
- **Per-row path** — :func:`datacustomcode.client.named_credential_request_col`
  fans the callout out across the DataFrame, one call per row, returning a struct
  Column ``{status, response, error_code, error_message}`` where ``response`` is
  itself ``{status_code, body, headers}``. A non-2xx response is still
  ``SUCCESS`` with its code in ``response.status_code``; a transport failure sets
  ``status`` to ``ERROR`` without aborting the whole Spark job.
"""

import json
import logging

from pyspark.sql import Column
from pyspark.sql.functions import (
    array,
    col,
    concat,
    get_json_object,
    lit,
    struct,
    to_json,
)

from datacustomcode.client import Client, named_credential_request_col
from datacustomcode.io.writer.base import WriteMode
from datacustomcode.named_credential.types.http_method import HTTPMethod
from datacustomcode.named_credential.types.http_request_builder import (
    HTTPRequestBuilder,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CALLOUT_URL = "callout:gemini"

_TEXT_COLUMN = "Description__c"
_SOURCE_DLO = "Account_std__dll"
_TARGET_DLO = "Account_std_copy__dll"

_PROMPT = (
    "Summarize the following text in one sentence. Respond with the summary "
    "only, no preamble.\n\nText:\n"
)

_REQUEST = (
    HTTPRequestBuilder()
    .set_url(CALLOUT_URL)
    .set_method(HTTPMethod.POST)
    .set_headers({"Content-Type": "application/json", "Accept": "application/json"})
    .set_response_timeout_seconds(60)
    .build()
)


def _gemini_body_col(text_col: Column) -> Column:
    """Build a per-row Gemini ``generateContent`` request body as a JSON string.

    Using ``to_json(struct(...))`` keeps the row text properly escaped inside the
    JSON payload rather than string-concatenating it.
    """
    prompt = concat(lit(_PROMPT), text_col)
    contents = array(struct(array(struct(prompt.alias("text"))).alias("parts")))
    return to_json(struct(contents.alias("contents")))


def _gemini_body(text: str) -> str:
    """Build a Gemini ``generateContent`` request body as a JSON string (driver)."""
    return json.dumps({"contents": [{"parts": [{"text": _PROMPT + text}]}]})


def _summarize_on_driver(client: Client, text: str) -> str:
    """One-shot driver callout: summarize a single string once, not per row.

    The scalar counterpart to the per-row column path — same request template,
    but dispatched once on the driver and returning an ``HTTPResponse``.
    """
    response = client.named_credential_request(_REQUEST, body=_gemini_body(text))

    # Don't raise: a failed driver callout shouldn't abort the whole job.
    if not response.is_success:
        logger.error(f"Driver Gemini callout failed: HTTP {response.status_code}")
        return ""

    envelope = json.loads(response.body) if response.body else {}
    try:
        return str(envelope["candidates"][0]["content"]["parts"][0]["text"])
    except (KeyError, IndexError, TypeError):
        return ""


def main():
    client = Client()

    df = client.read_dlo(_SOURCE_DLO)

    # Driver path: one callout on the driver over a single representative row.
    sample = df.select(_TEXT_COLUMN).first()
    if sample and sample[0]:
        driver_summary = _summarize_on_driver(client, sample[0])
        logger.info(f"Driver-path sample summary: {driver_summary}")

    # Per-row path: one Gemini callout per row; the result struct is a column.
    callout = named_credential_request_col(
        _REQUEST, body=_gemini_body_col(col(_TEXT_COLUMN))
    )
    df = df.withColumn("_callout", callout)

    # Pull the model's text out of the response body. A row whose callout failed
    # (non-2xx or transport error) yields null here rather than failing the job.
    summary = get_json_object(
        col("_callout")["response"]["body"],
        "$.candidates[0].content.parts[0].text",
    )

    df = df.select(
        col("id__c").alias("id__c"),
        col("Description__c").alias("description__c"),
        col("kq_id__c").alias("kq_id__c"),
        summary.alias("summary__c"),
        col("_callout")["status"].alias("callout_status__c"),
        col("_callout")["response"]["status_code"].alias("callout_http_code__c"),
    )

    client.write_to_dlo(_TARGET_DLO, df, write_mode=WriteMode.APPEND)


if __name__ == "__main__":
    main()
