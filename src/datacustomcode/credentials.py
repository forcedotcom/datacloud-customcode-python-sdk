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
from __future__ import annotations

import configparser
from dataclasses import dataclass, field
from enum import Enum
import os
from typing import Optional, Tuple

from loguru import logger
import requests

INI_FILE = os.path.expanduser("~/.datacustomcode/credentials.ini")


class AuthType(str, Enum):
    """Supported authentication methods for Salesforce Data Cloud."""

    USERNAME_PASSWORD = "username_password"
    OAUTH = "oauth"


# Environment variable mappings for each auth type
ENV_CREDENTIALS_COMMON = {
    "login_url": "SFDC_LOGIN_URL",
    "client_id": "SFDC_CLIENT_ID",
}

ENV_CREDENTIALS_USERNAME_PASSWORD = {
    "username": "SFDC_USERNAME",
    "password": "SFDC_PASSWORD",
    "client_secret": "SFDC_CLIENT_SECRET",
}

ENV_CREDENTIALS_OAUTH = {
    "client_secret": "SFDC_CLIENT_SECRET",
}


def fetch_oauth_token(
    login_url: str,
    client_id: str,
    client_secret: str,
) -> Tuple[str, str]:
    """Fetch OAuth token using Client Credentials flow.

    This function authenticates using the OAuth 2.0 Client Credentials grant type.
    It makes a POST request to the Salesforce OAuth endpoint to obtain an access token.

    Args:
        login_url: Salesforce login URL (e.g., https://login.salesforce.com)
        client_id: Connected App client ID
        client_secret: Connected App client secret

    Returns:
        Tuple of (core_token, instance_url)

    Raises:
        requests.HTTPError: If the token request fails
        ValueError: If the response doesn't contain expected fields
    """
    token_url = f"{login_url}/services/oauth2/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    logger.debug(f"Fetching OAuth token from {token_url}")

    response = requests.post(url=token_url, params=params)
    print(f"response: {response.text}")  # [to be removed]

    if response.status_code == 200:
        token_data = response.json()
        core_token = token_data.get("access_token")
        instance_url = token_data.get("instance_url")

        if not core_token:
            raise ValueError("OAuth response missing 'access_token' field")
        if not instance_url:
            raise ValueError("OAuth response missing 'instance_url' field")

        logger.debug("Successfully obtained OAuth token")
        return core_token, instance_url
    else:
        error_msg = f"OAuth token request failed with code {response.status_code}"
        try:
            error_detail = response.json()
            error_msg += f": {error_detail}"
        except Exception:
            error_msg += f": {response.text}"
        raise requests.HTTPError(error_msg, response=response)


@dataclass
class Credentials:
    """Flexible credentials supporting multiple authentication methods.

    Supports two authentication methods:
    - OAUTH: OAuth 2.0 client credentials flow (default, simplest)
    - USERNAME_PASSWORD: Traditional username/password OAuth flow
    """

    # Required for all auth types
    login_url: str
    client_id: str
    auth_type: AuthType = field(default=AuthType.OAUTH)

    # Username/Password flow fields
    username: Optional[str] = None
    password: Optional[str] = None

    # Common field (used by both auth types)
    client_secret: Optional[str] = None

    def __post_init__(self):
        """Validate credentials based on auth_type."""
        self._validate()

    def _validate(self) -> None:
        """Validate that required fields are present for the auth type."""
        if self.auth_type == AuthType.USERNAME_PASSWORD:
            missing = []
            if not self.username:
                missing.append("username")
            if not self.password:
                missing.append("password")
            if not self.client_secret:
                missing.append("client_secret")
            if missing:
                raise ValueError(
                    f"Username/Password auth requires: {', '.join(missing)}"
                )

        elif self.auth_type == AuthType.OAUTH:
            if not self.client_secret:
                raise ValueError("OAuth auth requires: client_secret")

    def get_oauth_token(self) -> Tuple[str, str]:
        """Fetch OAuth token using client credentials flow.

        This method fetches a core_token that can be used with the CDP connector.
        It uses the client_id and client_secret to obtain a token.

        Returns:
            Tuple of (core_token, instance_url)

        Raises:
            ValueError: If auth_type is not OAUTH
            requests.HTTPError: If the token request fails
        """
        if self.auth_type != AuthType.OAUTH:
            raise ValueError("get_oauth_token() is only for OAUTH auth type")

        if self.client_secret is None:
            raise ValueError("client_secret is required for OAUTH auth type")

        return fetch_oauth_token(
            self.login_url,
            self.client_id,
            self.client_secret,
        )

    @classmethod
    def from_ini(
        cls,
        profile: str = "default",
        ini_file: str = INI_FILE,
    ) -> Credentials:
        """Load credentials from INI file.

        Args:
            profile: Profile section name in the INI file (default: "default")
            ini_file: Path to the credentials INI file

        Returns:
            Credentials instance loaded from the INI file

        Raises:
            KeyError: If the profile or required fields are missing
        """
        config = configparser.ConfigParser()
        expanded_ini_file = os.path.expanduser(ini_file)
        logger.debug(f"Reading {expanded_ini_file} for profile {profile}")

        if not os.path.exists(expanded_ini_file):
            raise FileNotFoundError(f"Credentials file not found: {expanded_ini_file}")

        config.read(expanded_ini_file)

        if profile not in config:
            raise KeyError(f"Profile '{profile}' not found in {expanded_ini_file}")

        section = config[profile]

        # Determine auth type (default to oauth)
        auth_type_str = section.get("auth_type", AuthType.OAUTH.value)
        try:
            auth_type = AuthType(auth_type_str)
        except ValueError as exc:
            raise ValueError(
                f"Invalid auth_type '{auth_type_str}' in profile '{profile}'. "
                f"Valid options: {[t.value for t in AuthType]}"
            ) from exc

        return cls(
            login_url=section["login_url"],
            client_id=section["client_id"],
            auth_type=auth_type,
            # Username/Password fields
            username=section.get("username"),
            password=section.get("password"),
            client_secret=section.get("client_secret"),
        )

    @classmethod
    def from_env(cls) -> Credentials:
        """Load credentials from environment variables.

        Environment variables:
            Common (required):
                SFDC_LOGIN_URL: Salesforce login URL
                SFDC_CLIENT_ID: Connected App client ID
                SFDC_AUTH_TYPE: Authentication type (optional, defaults to oauth)

            For oauth (default):
                SFDC_CLIENT_SECRET: Connected App client secret

            For username_password:
                SFDC_USERNAME: Salesforce username
                SFDC_PASSWORD: Salesforce password
                SFDC_CLIENT_SECRET: Connected App client secret

        Returns:
            Credentials instance loaded from environment variables

        Raises:
            ValueError: If required environment variables are missing
        """
        # Check for common required variables
        login_url = os.environ.get("SFDC_LOGIN_URL")
        client_id = os.environ.get("SFDC_CLIENT_ID")

        if not login_url or not client_id:
            raise ValueError(
                "Environment variables SFDC_LOGIN_URL and SFDC_CLIENT_ID are required."
            )

        # Determine auth type
        auth_type_str = os.environ.get("SFDC_AUTH_TYPE", AuthType.OAUTH.value)
        try:
            auth_type = AuthType(auth_type_str)
        except ValueError as exc:
            raise ValueError(
                f"Invalid SFDC_AUTH_TYPE '{auth_type_str}'. "
                f"Valid options: {[t.value for t in AuthType]}"
            ) from exc

        return cls(
            login_url=login_url,
            client_id=client_id,
            auth_type=auth_type,
            # Username/Password fields
            username=os.environ.get("SFDC_USERNAME"),
            password=os.environ.get("SFDC_PASSWORD"),

            client_secret=os.environ.get("SFDC_CLIENT_SECRET"),
        )

    @classmethod
    def from_available(cls, profile: str = "default") -> Credentials:
        """Load credentials from the first available source.

        Checks sources in order:
        1. Environment variables (if SFDC_LOGIN_URL is set)
        2. INI file (~/.datacustomcode/credentials.ini)

        Args:
            profile: Profile name to use when loading from INI file

        Returns:
            Credentials instance from the first available source

        Raises:
            ValueError: If no credentials are found in any source
        """
        # Check environment variables first
        if os.environ.get("SFDC_LOGIN_URL"):
            logger.debug("Loading credentials from environment variables")
            return cls.from_env()

        # Check INI file
        if os.path.exists(os.path.expanduser(INI_FILE)):
            logger.debug(f"Loading credentials from INI file: {INI_FILE}")
            return cls.from_ini(profile=profile)

        raise ValueError(
            "Credentials not found in environment or INI file. "
            "Run `datacustomcode configure` to create a credentials file."
        )

    def update_ini(self, profile: str = "default", ini_file: str = INI_FILE) -> None:
        """Save credentials to INI file.

        Args:
            profile: Profile section name in the INI file
            ini_file: Path to the credentials INI file
        """
        config = configparser.ConfigParser()

        expanded_ini_file = os.path.expanduser(ini_file)
        os.makedirs(os.path.dirname(expanded_ini_file), exist_ok=True)

        if os.path.exists(expanded_ini_file):
            config.read(expanded_ini_file)

        if profile not in config:
            config[profile] = {}

        # Always save common fields
        config[profile]["auth_type"] = self.auth_type.value
        config[profile]["login_url"] = self.login_url
        config[profile]["client_id"] = self.client_id

        # Save fields based on auth type
        if self.auth_type == AuthType.USERNAME_PASSWORD:
            config[profile]["username"] = self.username or ""
            config[profile]["password"] = self.password or ""
            config[profile]["client_secret"] = self.client_secret or ""

        elif self.auth_type == AuthType.OAUTH:
            config[profile]["client_secret"] = self.client_secret or ""
            # Remove fields from username_password auth type
            for key in ["username", "password"]:
                config[profile].pop(key, None)

        with open(expanded_ini_file, "w") as f:
            config.write(f)

        logger.debug(f"Saved credentials to {expanded_ini_file} [{profile}]")
