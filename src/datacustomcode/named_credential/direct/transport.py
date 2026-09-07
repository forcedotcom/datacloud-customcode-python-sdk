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
"""Send a Named Credential callout over HTTP.

Resolves the ``callout:<NamedCredential>/<path>`` reference to a real endpoint
(via the Named Credential Connect API, falling back to ``target_url`` in
``external_callout_config.json``), attaches the credential's auth, and returns the raw
``{status_code, headers, body}`` response.
"""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    Optional,
)

import requests

from datacustomcode.named_credential.direct.auth import DynamicAuthHandler
from datacustomcode.named_credential.direct.credentials import CredentialStore
from datacustomcode.named_credential.direct.url_resolver import resolve_base_url
from datacustomcode.named_credential.errors import NamedCredentialCallError
from datacustomcode.token_provider import (
    CredentialsTokenProvider,
    SFCLITokenProvider,
    TokenProvider,
)


class DirectCalloutTransport:
    def __init__(
        self,
        credentials_profile: str = "default",
        sf_cli_org: Optional[str] = None,
        credential_file: Optional[str] = None,
    ) -> None:
        self._store = CredentialStore(credential_file)
        self._token_provider = self._build_token_provider(
            credentials_profile, sf_cli_org
        )
        # Resolved base URL per callout key. Stable for the transport's life, so
        # cache it to avoid a token fetch + Connect API call on every row
        self._base_url_cache: Dict[str, str] = {}

    @staticmethod
    def _build_token_provider(
        credentials_profile: str, sf_cli_org: Optional[str]
    ) -> TokenProvider:
        if sf_cli_org:
            return SFCLITokenProvider(sf_cli_org)
        return CredentialsTokenProvider(credentials_profile)

    def callout(self, callout_request: Dict[str, Any]) -> Dict[str, Any]:
        raw_url = callout_request["path"]
        if not raw_url.startswith("callout:"):
            raise NamedCredentialCallError(
                f"Callout URL must start with 'callout:', got '{raw_url}'."
            )

        # Split the named credential reference at the first '/' or '?'; the remainder
        # path or query string is appended to the resolved base URL verbatim.
        sep_idx = min(
            (i for i in (raw_url.find("/"), raw_url.find("?")) if i != -1),
            default=len(raw_url),
        )
        callout_key = raw_url[:sep_idx]
        path_suffix = raw_url[sep_idx:]

        if callout_key == "callout:":
            raise NamedCredentialCallError(
                f"Named Credential name is empty in URL '{raw_url}'."
            )

        cred_config = self._store.get(callout_key)
        base_url = self._base_url_cache.get(callout_key)
        if base_url is None:
            base_url = resolve_base_url(callout_key, cred_config, self._token_provider)
            self._base_url_cache[callout_key] = base_url

        body = callout_request.get("body") or None
        # Headers are passed; the SDK assumes no Content-Type.
        headers = dict(callout_request.get("headers", {}))

        response = requests.request(
            method=callout_request["method"],
            url=base_url + path_suffix,
            headers=headers,
            data=body,
            auth=DynamicAuthHandler(cred_config),
            timeout=30,
        )
        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response.text,
        }
