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
"""Load developer credentials for direct callouts from ``external_callout_config.json``.

The file lives in the parent of the payload folder so it is never packaged in
the deployment zip. Its ``credentials`` section is keyed by callout reference
(``callout:<NamedCredential>``); each value carries a mandatory ``auth_type``
and an optional ``target_url``.
"""

from __future__ import annotations

from enum import Enum
import json
import os
from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

# Default file name; discovered in the parent of the payload folder.
EXTERNAL_CALLOUT_CREDENTIAL = "external_callout_config.json"
# Absolute-path override, primarily for tests and non-standard layouts.
CREDENTIAL_FILE_ENV_VAR = "DATACUSTOMCODE_EXTERNAL_CALLOUT_CONFIG"


class AuthType(str, Enum):
    """External Credential auth types supported by External Services."""

    BASIC = "Basic"
    CUSTOM = "Custom"
    JWT = "Jwt"
    OAUTH = "OAuth"


class CredentialError(RuntimeError):
    """Raised when credentials cannot be found or are invalid."""


def _discover_credential_file() -> Optional[Path]:
    """Find the config file via env override, then by walking up from cwd."""
    override = os.environ.get(CREDENTIAL_FILE_ENV_VAR)
    if override:
        return Path(override)

    for directory in (Path.cwd(), *Path.cwd().parents):
        candidate = directory / EXTERNAL_CALLOUT_CREDENTIAL
        if candidate.is_file():
            return candidate
    return None


class CredentialStore:
    """Reads ``external_callout_config.json`` and returns per-callout config."""

    def __init__(self, credential_file: Optional[str] = None) -> None:
        self._explicit_path = Path(credential_file) if credential_file else None
        self._credentials: Optional[Dict[str, Dict[str, Any]]] = None

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if self._credentials is not None:
            return self._credentials

        path = self._explicit_path or _discover_credential_file()
        if path is None or not path.is_file():
            raise CredentialError(
                f"Could not find '{EXTERNAL_CALLOUT_CREDENTIAL}'. Place it in the "
                f"parent of your payload folder, or set "
                f"${CREDENTIAL_FILE_ENV_VAR} to its path."
            )
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            raise CredentialError(f"Failed to read '{path}': {exc}") from exc

        # Per-callout entries live under the ``credentials`` section, leaving
        # room for other config sections alongside them in the future.
        credentials = data.get("credentials") if isinstance(data, dict) else None
        if not isinstance(credentials, dict):
            raise CredentialError(
                f"'{path}' must be a JSON object with a 'credentials' section "
                f"keyed by callout reference."
            )
        self._credentials = credentials
        return credentials

    def get(self, callout_key: str) -> Dict[str, Any]:
        """Return the config for a callout key (e.g. ``callout:AWS_S3_Service``).

        Raises:
            CredentialError: if the key is missing or has no ``auth_type``.
        """
        credentials = self._load()
        config = credentials.get(callout_key)
        if config is None:
            raise CredentialError(
                f"No credential configuration found for '{callout_key}'. "
                f"Add it to the 'credentials' section of "
                f"'{EXTERNAL_CALLOUT_CREDENTIAL}'."
            )
        if not isinstance(config, dict) or not config.get("auth_type"):
            raise CredentialError(
                f"Credential for '{callout_key}' is missing the mandatory "
                f"'auth_type' field."
            )
        return config
