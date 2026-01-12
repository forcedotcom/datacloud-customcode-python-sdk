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
from typing import Optional

from loguru import logger

INI_FILE = os.path.expanduser("~/.datacustomcode/credentials.ini")


class AuthType(str, Enum):
    """Supported authentication methods for Salesforce Data Cloud."""

    USERNAME_PASSWORD = "username_password"
    OAUTH_TOKENS = "oauth_tokens"


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

ENV_CREDENTIALS_OAUTH_TOKENS = {
    "client_secret": "SFDC_CLIENT_SECRET",
    "refresh_token": "SFDC_REFRESH_TOKEN",
    "core_token": "SFDC_CORE_TOKEN",
}


@dataclass
class Credentials:
    """Flexible credentials supporting multiple authentication methods.

    Supports two authentication methods:
    - OAUTH_TOKENS: OAuth tokens (core_token and refresh_token) authentication
    - USERNAME_PASSWORD: Traditional username/password OAuth flow
    """

    # Required for all auth types
    login_url: str
    client_id: str
    auth_type: AuthType = field(default=AuthType.OAUTH_TOKENS)

    # Username/Password flow fields
    username: Optional[str] = None
    password: Optional[str] = None

    # Common field
    client_secret: Optional[str] = None

    # OAuth Tokens flow fields
    core_token: Optional[str] = None
    refresh_token: Optional[str] = None

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

        elif self.auth_type == AuthType.OAUTH_TOKENS:
            missing = []
            if not self.refresh_token:
                missing.append("refresh_token")
            if not self.client_secret:
                missing.append("client_secret")
            if missing:
                raise ValueError(f"OAuth Tokens auth requires: {', '.join(missing)}")

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

        # Determine auth type (default to oauth_tokens)
        auth_type_str = section.get("auth_type", AuthType.OAUTH_TOKENS.value)
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
            # OAuth Tokens fields
            core_token=section.get("core_token"),
            refresh_token=section.get("refresh_token"),
        )

    @classmethod
    def from_env(cls) -> Credentials:
        """Load credentials from environment variables.

        Environment variables:
            Common (required):
                SFDC_LOGIN_URL: Salesforce login URL
                SFDC_CLIENT_ID: Connected App client ID
                SFDC_AUTH_TYPE: Authentication type (optional, defaults to oauth_tokens)

            For oauth_tokens (default):
                SFDC_CLIENT_SECRET: Connected App client secret
                SFDC_REFRESH_TOKEN: OAuth refresh token
                SFDC_CORE_TOKEN: OAuth core/access token (optional)

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
        auth_type_str = os.environ.get("SFDC_AUTH_TYPE", AuthType.OAUTH_TOKENS.value)
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
            # OAuth Tokens fields
            core_token=os.environ.get("SFDC_CORE_TOKEN"),
            refresh_token=os.environ.get("SFDC_REFRESH_TOKEN"),
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
            # Remove fields from other auth types
            for key in ["refresh_token", "core_token"]:
                config[profile].pop(key, None)

        elif self.auth_type == AuthType.OAUTH_TOKENS:
            config[profile]["client_secret"] = self.client_secret or ""
            config[profile]["refresh_token"] = self.refresh_token or ""
            if self.core_token:
                config[profile]["core_token"] = self.core_token
            # Remove fields from other auth types
            for key in ["username", "password"]:
                config[profile].pop(key, None)

        with open(expanded_ini_file, "w") as f:
            config.write(f)

        logger.debug(f"Saved credentials to {expanded_ini_file} [{profile}]")
