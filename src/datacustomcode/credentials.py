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
from dataclasses import dataclass
import os

from loguru import logger

ENV_CREDENTIALS = {
    "username": "SFDC_USERNAME",
    "password": "SFDC_PASSWORD",
    "client_id": "SFDC_CLIENT_ID",
    "client_secret": "SFDC_CLIENT_SECRET",
    "login_url": "SFDC_LOGIN_URL",
}
INI_FILE = os.path.expanduser("~/.datacustomcode/credentials.ini")


@dataclass
class Credentials:
    username: str
    password: str
    client_id: str
    client_secret: str
    login_url: str

    @classmethod
    def from_ini(
        cls,
        profile: str = "default",
        ini_file: str = INI_FILE,
    ) -> Credentials:
        config = configparser.ConfigParser()
        logger.debug(f"Reading {ini_file} for profile {profile}")
        config.read(ini_file)
        return cls(
            username=config[profile]["username"],
            password=config[profile]["password"],
            client_id=config[profile]["client_id"],
            client_secret=config[profile]["client_secret"],
            login_url=config[profile]["login_url"],
        )

    @classmethod
    def from_env(cls) -> Credentials:
        try:
            return cls(**{k: os.environ[v] for k, v in ENV_CREDENTIALS.items()})
        except KeyError as exc:
            raise ValueError(
                f"All of {ENV_CREDENTIALS.values()} must be set in environment."
            ) from exc

    @classmethod
    def from_available(cls) -> Credentials:
        if os.environ.get("SFDC_USERNAME"):
            return cls.from_env()
        if os.path.exists(INI_FILE):
            return cls.from_ini()
        raise ValueError(
            "Credentials not found in env or ini file. "
            "Run `datacustomcode configure` to create a credentials file."
        )

    def update_ini(self, profile: str = "default", ini_file: str = INI_FILE):
        config = configparser.ConfigParser()

        expanded_ini_file = os.path.expanduser(ini_file)
        os.makedirs(os.path.dirname(expanded_ini_file), exist_ok=True)

        if os.path.exists(expanded_ini_file):
            config.read(expanded_ini_file)

        if profile not in config:
            config[profile] = {}

        config[profile]["username"] = self.username
        config[profile]["password"] = self.password
        config[profile]["client_id"] = self.client_id
        config[profile]["client_secret"] = self.client_secret
        config[profile]["login_url"] = self.login_url

        with open(expanded_ini_file, "w") as f:
            config.write(f)
