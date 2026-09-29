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
Inject external credential auth into an outgoing request.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
)
import urllib.parse

from requests.auth import AuthBase

from datacustomcode.named_credential.direct.credentials import AuthType

if TYPE_CHECKING:
    from requests.models import PreparedRequest

_SIGV4_ALGORITHM = "AWS4-HMAC-SHA256"


class DynamicAuthHandler(AuthBase):
    def __init__(self, cred_config: Dict[str, Any]) -> None:
        self.config = cred_config
        self.auth_type = cred_config.get("auth_type")

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.auth_type == AuthType.BASIC.value:
            user = self.config.get("username", "")
            pwd = self.config.get("password", "")
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            request.headers["Authorization"] = f"Basic {token}"

        elif self.auth_type == AuthType.CUSTOM.value:
            for name, value in self.config.get("custom_headers", {}).items():
                request.headers[name] = value

        elif self.auth_type in (AuthType.OAUTH.value, AuthType.JWT.value):
            bearer = self.config.get("access_token") or self.config.get("token")
            if not bearer:
                raise ValueError(
                    f"'{self.auth_type}' auth requires an 'access_token' or 'token'."
                )
            request.headers["Authorization"] = f"Bearer {bearer}"

        elif self.auth_type == AuthType.AWS_SIG_V4.value:
            self._sign_aws_sigv4(request)

        else:
            raise ValueError(f"Unsupported auth_type '{self.auth_type}'.")

        return request

    def _sign_aws_sigv4(self, request: PreparedRequest) -> None:
        """Sign ``request`` with AWS Signature Version 4.

        Requires ``aws_access_key_id``, ``aws_secret_access_key``, ``aws_region``,
        and ``aws_service`` in the credential config; ``aws_session_token`` is
        optional (for temporary credentials). The signed date, payload hash, and
        (when present) session token are added as ``x-amz-*`` headers so the sent
        request matches what was signed.
        """
        access_key = self.config.get("aws_access_key_id")
        secret_key = self.config.get("aws_secret_access_key")
        region = self.config.get("aws_region")
        service = self.config.get("aws_service")
        session_token = self.config.get("aws_session_token")
        missing = [
            name
            for name, value in (
                ("aws_access_key_id", access_key),
                ("aws_secret_access_key", secret_key),
                ("aws_region", region),
                ("aws_service", service),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"'{self.auth_type}' auth requires {', '.join(missing)}.")
        access_key = str(access_key)
        secret_key = str(secret_key)
        region = str(region)
        service = str(service)

        parsed = urllib.parse.urlsplit(str(request.url or ""))
        host = parsed.netloc
        canonical_uri = urllib.parse.quote(parsed.path or "/", safe="/-_.~")
        canonical_query = _canonical_query_string(parsed.query)

        body = request.body or b""
        if isinstance(body, str):
            body = body.encode("utf-8")
        payload_hash = hashlib.sha256(body).hexdigest()

        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        request.headers["x-amz-date"] = amz_date
        request.headers["x-amz-content-sha256"] = payload_hash
        if session_token:
            request.headers["x-amz-security-token"] = session_token

        signed = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if session_token:
            signed["x-amz-security-token"] = session_token
        signed_headers = ";".join(sorted(signed))
        canonical_headers = "".join(
            f"{name}:{signed[name]}\n" for name in sorted(signed)
        )

        canonical_request = "\n".join(
            [
                request.method or "GET",
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{datestamp}/{region}/{service}/aws4_request"
        string_to_sign = "\n".join(
            [
                _SIGV4_ALGORITHM,
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = _derive_signing_key(secret_key, datestamp, region, service)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        request.headers["Authorization"] = (
            f"{_SIGV4_ALGORITHM} Credential={access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )


def _canonical_query_string(query: str) -> str:
    """Build the AWS Sig V4 canonical query string from a raw query string."""
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = [
        (
            urllib.parse.quote(key, safe="-_.~"),
            urllib.parse.quote(value, safe="-_.~"),
        )
        for key, value in pairs
    ]
    encoded.sort()
    return "&".join(f"{key}={value}" for key, value in encoded)


def _derive_signing_key(
    secret_key: str, datestamp: str, region: str, service: str
) -> bytes:
    """Derive the AWS Sig V4 signing key via the chained HMAC-SHA256 sequence."""

    def _hmac(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _hmac(f"AWS4{secret_key}".encode(), datestamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    return _hmac(k_service, "aws4_request")
