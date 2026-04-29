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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datacustomcode.deploy import AccessTokenResponse


class TokenProvider(ABC):
    """Abstract base class for providing authentication tokens."""

    @abstractmethod
    def get_token(self) -> "AccessTokenResponse":
        """Retrieve a fresh access token and instance URL.

        Returns:
            AccessTokenResponse containing access_token and instance_url
        """
        ...


class CredentialsTokenProvider(TokenProvider):
    """Token provider that uses stored credentials with refresh token flow."""

    def __init__(self, credentials_profile: str = "default"):
        self.credentials_profile = credentials_profile

    def get_token(self) -> "AccessTokenResponse":
        """Get token by refreshing credentials from stored profile."""
        import requests

        from datacustomcode.credentials import AuthType, Credentials
        from datacustomcode.deploy import AccessTokenResponse

        # Load credentials (freshly, not cached)
        credentials = Credentials.from_available(profile=self.credentials_profile)

        token_url = f"{credentials.login_url.rstrip('/')}/services/oauth2/token"

        if credentials.auth_type == AuthType.OAUTH_TOKENS:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": credentials.refresh_token,
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
            }
        elif credentials.auth_type == AuthType.CLIENT_CREDENTIALS:
            data = {
                "grant_type": "client_credentials",
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
            }
        else:
            raise RuntimeError(f"Unsupported auth_type: {credentials.auth_type}")

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            instance_url = token_data.get("instance_url")

            if not access_token or not instance_url:
                raise RuntimeError(
                    "Token refresh response missing access_token or instance_url"
                )

            return AccessTokenResponse(
                access_token=access_token, instance_url=instance_url
            )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get access token: {e}") from e


class SFCLITokenProvider(TokenProvider):
    """Token provider that uses Salesforce CLI for authentication."""

    def __init__(self, sf_cli_org: str):
        self.sf_cli_org = sf_cli_org

    def get_token(self) -> "AccessTokenResponse":
        """Get token from Salesforce SF CLI"""
        import json
        import subprocess

        from datacustomcode.deploy import AccessTokenResponse

        try:
            result = subprocess.run(
                ["sf", "org", "display", "--target-org", self.sf_cli_org, "--json"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "The 'sf' command was not found. "
                "Install Salesforce CLI: https://developer.salesforce.com/tools/salesforcecli"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"'sf org display' timed out for org '{self.sf_cli_org}'"
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"'sf org display' failed for org '{self.sf_cli_org}': {exc.stderr}"
            ) from exc

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse JSON from 'sf org display': {result.stdout}"
            ) from exc

        if data.get("status") != 0:
            raise RuntimeError(
                f"SF CLI error for org '{self.sf_cli_org}': "
                f"{data.get('message', 'unknown error')}"
            )

        result_data = data.get("result", {})
        access_token = result_data.get("accessToken")
        instance_url = result_data.get("instanceUrl")

        if not access_token or not instance_url:
            raise RuntimeError(
                f"'sf org display' did not return an access token or instance URL "
                f"for org '{self.sf_cli_org}'"
            )

        return AccessTokenResponse(access_token=access_token, instance_url=instance_url)
