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
"""Resolve a ``callout:<NamedCredential>`` reference to a real base URL.

Prefers the NamedCredential Connect API ``calloutUrl``; falls back to the
developer-supplied ``target_url`` when the API is unavailable or the Named
Credential is not yet onboarded.
"""

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

import requests

from datacustomcode.named_credential.direct.credentials import CredentialError

if TYPE_CHECKING:
    from datacustomcode.token_provider import TokenProvider

logger = logging.getLogger(__name__)

NAMED_CREDENTIAL_PATH = (
    "services/data/v63.0/named-credentials/named-credential-setup/{name}"
)


def _callout_url_from_connect_api(
    developer_name: str, token_provider: TokenProvider
) -> Optional[str]:
    """Fetch ``calloutUrl`` for a Named Credential, or None on any failure."""
    try:
        token = token_provider.get_token()
    except Exception as exc:
        # No usable token (e.g. not logged in) — expected; fall back quietly.
        logger.debug("Could not obtain a token for %s: %s", developer_name, exc)
        return None

    try:
        path = NAMED_CREDENTIAL_PATH.format(name=developer_name)
        url = f"{token.instance_url.rstrip('/')}/{path}"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token.access_token}"},
            timeout=30,
        )
        response.raise_for_status()
        callout_url = response.json().get("calloutUrl")
        return callout_url or None
    except Exception as exc:
        # A token was obtained but the Connect API call failed — likely a
        # misconfiguration (named credential not onboarded, missing
        # permission, wrong org). Surface it before falling back to target_url.
        logger.warning(
            "Connect API URL resolution failed for %s: %s. "
            "Falling back to 'target_url' from external_callout_config.json if set.",
            developer_name,
            exc,
        )
        return None


def resolve_base_url(
    callout_key: str,
    cred_config: Dict[str, Any],
    token_provider: Optional[TokenProvider],
) -> str:
    """Resolve the base URL for a callout key.

    Args:
        callout_key: e.g. ``callout:Nominatim_Geocoding``.
        cred_config: The callout's ``external_callout_config.json`` entry.
        token_provider: Provides a token/instance URL for the Connect API; when
            None, only ``target_url`` is used.

    Raises:
        CredentialError: if no URL can be resolved.
    """
    developer_name = callout_key.split(":", 1)[1] if ":" in callout_key else callout_key

    base_url: Optional[str] = None
    if token_provider is not None:
        base_url = _callout_url_from_connect_api(developer_name, token_provider)

    if not base_url:
        base_url = cred_config.get("target_url") or None

    if not base_url:
        raise CredentialError(
            f"Could not resolve a URL for '{callout_key}'. Ensure the Named "
            f"Credential exists (sf login) or set 'target_url' in "
            f"external_callout_config.json."
        )
    return base_url.rstrip("/")
