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

    OAUTH_TOKENS = "oauth_tokens"
    CLIENT_CREDENTIALS = "client_credentials"


@dataclass
class Credentials:
    """Flexible credentials supporting multiple authentication methods.

    Supports two authentication methods:
    - OAUTH_TOKENS: OAuth tokens (refresh_token) authentication (default)
    - CLIENT_CREDENTIALS: Server-to-server integration using client_id/secret only
    """

    # Required for all auth types
    login_url: str
    client_id: str
    client_secret: str
    auth_type: AuthType = field(default=AuthType.OAUTH_TOKENS)

    # OAuth Tokens flow fields
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    redirect_uri: Optional[str] = None

    def __post_init__(self):
        """Validate credentials based on auth_type."""
        self._validate()

    def _validate(self) -> None:
        """Validate that required fields are present for the auth type."""
        if self.auth_type == AuthType.OAUTH_TOKENS:
            missing = []
            if not self.refresh_token:
                missing.append("refresh_token")
            if not self.client_secret:
                missing.append("client_secret")
            if missing:
                raise ValueError(f"OAuth Tokens auth requires: {', '.join(missing)}")

        elif self.auth_type == AuthType.CLIENT_CREDENTIALS:
            if not self.client_secret:
                raise ValueError("Client Credentials auth requires: client_secret")

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
            client_secret=section["client_secret"],
            # OAuth Tokens fields
            access_token=section.get("access_token"),
            refresh_token=section.get("refresh_token"),
            redirect_uri=section.get("redirect_uri"),
        )

    @classmethod
    def from_env(cls) -> Credentials:
        """Load credentials from environment variables.

        Environment variables:
            Common (required):
                SFDC_LOGIN_URL: Salesforce login URL
                SFDC_CLIENT_ID: External Client App client ID
                SFDC_AUTH_TYPE: Authentication type (optional, defaults to oauth_tokens)

            For oauth_tokens (default):
                SFDC_CLIENT_SECRET: External Client App client secret
                SFDC_REFRESH_TOKEN: OAuth refresh token
                SFDC_ACCESS_TOKEN: OAuth access token (optional)

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
            client_secret=os.environ["SFDC_CLIENT_SECRET"],
            # OAuth Tokens fields
            access_token=os.environ.get("SFDC_ACCESS_TOKEN"),
            refresh_token=os.environ.get("SFDC_REFRESH_TOKEN"),
            redirect_uri=os.environ.get("SFDC_REDIRECT_URI"),
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
        config[profile]["client_secret"] = self.client_secret
        # Save fields based on auth type
        if self.auth_type == AuthType.OAUTH_TOKENS:
            config[profile]["refresh_token"] = self.refresh_token or ""
            config[profile]["redirect_uri"] = self.redirect_uri or ""
            if self.access_token:
                config[profile]["access_token"] = self.access_token
            # Remove fields from other auth types
            for key in ["username", "password"]:
                config[profile].pop(key, None)

        elif self.auth_type == AuthType.CLIENT_CREDENTIALS:
            # Remove fields from other auth types
            for key in [
                "username",
                "password",
                "refresh_token",
                "access_token",
                "redirect_uri",
            ]:
                config[profile].pop(key, None)

        with open(expanded_ini_file, "w") as f:
            config.write(f)

        # Set secure file permissions (0o600 - readable/writable by owner only)
        try:
            os.chmod(expanded_ini_file, 0o600)
        except OSError:
            # Ignore errors if we can't set file permissions (e.g., on Windows)
            pass

        logger.debug(f"Saved credentials to {expanded_ini_file} [{profile}]")
