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

import pytest

from datacustomcode.named_credential.base import NamedCredential
from datacustomcode.named_credential.default import DefaultNamedCredential
from datacustomcode.named_credential.types.http_request import HTTPRequest
from datacustomcode.named_credential.types.http_response import HTTPResponse
from datacustomcode.named_credential_config import (
    NamedCredentialConfig,
    NamedCredentialObjectConfig,
    named_credential_config,
)


class TestNamedCredentialConfig:
    def test_default_config_resolves_default_impl(self):
        assert named_credential_config.named_credential_config is not None
        instance = named_credential_config.named_credential_config.to_object()
        assert isinstance(instance, DefaultNamedCredential)

    def test_custom_implementation_is_discoverable(self):
        class CustomNamedCredential(NamedCredential):
            CONFIG_NAME = "CustomNamedCredential"

            def __init__(self, custom_param: str = "default", **kwargs):
                super().__init__(**kwargs)
                self.custom_param = custom_param

            def request(self, request, body=None):
                return HTTPResponse(status_code=200, body=self.custom_param)

        assert "CustomNamedCredential" in NamedCredential.available_config_names()
        cls = NamedCredential.subclass_from_config_name("CustomNamedCredential")
        assert cls is CustomNamedCredential

        config = NamedCredentialObjectConfig(
            type_config_name="CustomNamedCredential",
            options={"custom_param": "my_value"},
        )
        instance = config.to_object()
        assert isinstance(instance, CustomNamedCredential)

        response = instance.request(HTTPRequest(url="callout:NC/path"))
        assert response.body == "my_value"


class TestRuntimeNamedCredential:
    @pytest.fixture(autouse=True)
    def _reset_singleton(self):
        from datacustomcode.function.runtime import Runtime

        Runtime._instance = None
        yield
        Runtime._instance = None

    def test_runtime_exposes_default_named_credential(self):
        from datacustomcode.function.runtime import Runtime

        runtime = Runtime()
        assert isinstance(runtime.named_credential, DefaultNamedCredential)

    def test_named_credential_is_cached(self):
        from datacustomcode.function.runtime import Runtime

        runtime = Runtime()
        assert runtime.named_credential is runtime.named_credential

    def test_missing_config_raises(self, monkeypatch):
        from datacustomcode.function.runtime import Runtime

        monkeypatch.setattr(named_credential_config, "named_credential_config", None)
        runtime = Runtime()
        with pytest.raises(RuntimeError, match="Named Credential is not configured"):
            _ = runtime.named_credential


class TestNamedCredentialConfigUpdate:
    def test_update_prefers_other(self):
        base = NamedCredentialConfig(
            named_credential_config=NamedCredentialObjectConfig(
                type_config_name="DefaultNamedCredential"
            )
        )
        override = NamedCredentialConfig(
            named_credential_config=NamedCredentialObjectConfig(
                type_config_name="CustomNamedCredential"
            )
        )
        base.update(override)
        assert base.named_credential_config.type_config_name == "CustomNamedCredential"

    def test_update_keeps_forced_existing(self):
        base = NamedCredentialConfig(
            named_credential_config=NamedCredentialObjectConfig(
                type_config_name="DefaultNamedCredential", force=True
            )
        )
        override = NamedCredentialConfig(
            named_credential_config=NamedCredentialObjectConfig(
                type_config_name="CustomNamedCredential"
            )
        )
        base.update(override)
        assert base.named_credential_config.type_config_name == "DefaultNamedCredential"

    def test_update_keeps_existing_when_other_empty(self):
        base = NamedCredentialConfig(
            named_credential_config=NamedCredentialObjectConfig(
                type_config_name="DefaultNamedCredential"
            )
        )
        base.update(NamedCredentialConfig())
        assert base.named_credential_config.type_config_name == "DefaultNamedCredential"
