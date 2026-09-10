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

import base64
import json
from typing import ClassVar

import pytest
from requests.models import PreparedRequest

from datacustomcode.named_credential.direct.auth import DynamicAuthHandler
from datacustomcode.named_credential.direct.credentials import (
    CREDENTIAL_FILE_ENV_VAR,
    AuthType,
    CredentialError,
    CredentialStore,
)
from datacustomcode.named_credential.direct.url_resolver import resolve_base_url
from datacustomcode.named_credential.errors import NamedCredentialCallError


def _prepared_request() -> PreparedRequest:
    request = PreparedRequest()
    request.prepare(method="GET", url="https://example.com")
    return request


class TestDynamicAuthHandler:
    def test_basic_sets_authorization_header(self):
        handler = DynamicAuthHandler(
            {"auth_type": AuthType.BASIC.value, "username": "u", "password": "p"}
        )
        request = handler(_prepared_request())
        expected = base64.b64encode(b"u:p").decode()
        assert request.headers["Authorization"] == f"Basic {expected}"

    def test_custom_injects_custom_headers(self):
        handler = DynamicAuthHandler(
            {
                "auth_type": AuthType.CUSTOM.value,
                "custom_headers": {"X-Api-Key": "secret"},
            }
        )
        request = handler(_prepared_request())
        assert request.headers["X-Api-Key"] == "secret"

    def test_oauth_sets_bearer_from_access_token(self):
        handler = DynamicAuthHandler(
            {"auth_type": AuthType.OAUTH.value, "access_token": "tok"}
        )
        request = handler(_prepared_request())
        assert request.headers["Authorization"] == "Bearer tok"

    def test_jwt_sets_bearer_from_token(self):
        handler = DynamicAuthHandler(
            {"auth_type": AuthType.JWT.value, "token": "jwt-tok"}
        )
        request = handler(_prepared_request())
        assert request.headers["Authorization"] == "Bearer jwt-tok"

    def test_oauth_missing_token_raises(self):
        handler = DynamicAuthHandler({"auth_type": AuthType.OAUTH.value})
        with pytest.raises(ValueError):
            handler(_prepared_request())

    def test_jwt_missing_token_raises(self):
        handler = DynamicAuthHandler({"auth_type": AuthType.JWT.value})
        with pytest.raises(ValueError):
            handler(_prepared_request())

    def test_unsupported_auth_type_raises_value_error(self):
        handler = DynamicAuthHandler({"auth_type": "Nonsense"})
        with pytest.raises(ValueError):
            handler(_prepared_request())


def _reference_sigv4(config, method, url, body):
    """Independent SigV4 reference used to validate DynamicAuthHandler output."""
    import datetime
    import hashlib
    import hmac
    import urllib.parse

    parsed = urllib.parse.urlsplit(url)
    payload = body.encode("utf-8") if isinstance(body, str) else (body or b"")
    payload_hash = hashlib.sha256(payload).hexdigest()
    now = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")

    signed = {
        "host": parsed.netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    token = config.get("aws_session_token")
    if token:
        signed["x-amz-security-token"] = token
    signed_headers = ";".join(sorted(signed))
    canonical_headers = "".join(f"{n}:{signed[n]}\n" for n in sorted(signed))

    pairs = sorted(
        (urllib.parse.quote(k, safe="-_.~"), urllib.parse.quote(v, safe="-_.~"))
        for k, v in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    )
    canonical_query = "&".join(f"{k}={v}" for k, v in pairs)
    canonical_uri = urllib.parse.quote(parsed.path or "/", safe="/-_.~")
    canonical_request = "\n".join(
        [
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{datestamp}/{config['aws_region']}/{config['aws_service']}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    def _h(key, msg):
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k = _h(f"AWS4{config['aws_secret_access_key']}".encode(), datestamp)
    k = _h(k, config["aws_region"])
    k = _h(k, config["aws_service"])
    k = _h(k, "aws4_request")
    signature = hmac.new(k, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return (
        (
            f"AWS4-HMAC-SHA256 Credential={config['aws_access_key_id']}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
        amz_date,
        payload_hash,
    )


class TestAwsSigV4Auth:
    _CONFIG: ClassVar[dict] = {
        "auth_type": AuthType.AWS_SIG_V4.value,
        "aws_access_key_id": "AKIDEXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "aws_region": "us-east-1",
        "aws_service": "s3",
    }

    @staticmethod
    def _freeze_clock(monkeypatch):
        import datetime as _dt

        from datacustomcode.named_credential.direct import auth as auth_mod

        frozen = _dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=_dt.timezone.utc)

        class _FrozenDatetime(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return frozen if tz is None else frozen.astimezone(tz)

        monkeypatch.setattr(auth_mod.datetime, "datetime", _FrozenDatetime)

    def _sign(self, monkeypatch, config, method="GET", url=None, body=None):
        self._freeze_clock(monkeypatch)
        url = url or "https://bucket.s3.amazonaws.com/key"
        request = PreparedRequest()
        request.prepare(method=method, url=url, data=body)
        return DynamicAuthHandler(config)(request)

    def test_matches_reference_signature(self, monkeypatch):
        url = "https://bucket.s3.amazonaws.com/some/key?b=2&a=1"
        request = self._sign(monkeypatch, self._CONFIG, "PUT", url, "payload")
        expected, amz_date, payload_hash = _reference_sigv4(
            self._CONFIG, "PUT", url, "payload"
        )
        assert request.headers["Authorization"] == expected
        assert request.headers["x-amz-date"] == amz_date
        assert request.headers["x-amz-content-sha256"] == payload_hash
        assert "x-amz-security-token" not in request.headers

    def test_empty_body_hashes_empty_string(self, monkeypatch):
        request = self._sign(monkeypatch, self._CONFIG)
        assert (
            request.headers["x-amz-content-sha256"]
            == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

    def test_session_token_is_signed(self, monkeypatch):
        config = {**self._CONFIG, "aws_session_token": "SESSIONTOKEN"}
        request = self._sign(monkeypatch, config)
        assert request.headers["x-amz-security-token"] == "SESSIONTOKEN"
        assert "x-amz-security-token" in request.headers["Authorization"]
        expected, _, _ = _reference_sigv4(config, "GET", request.url, None)
        assert request.headers["Authorization"] == expected

    @pytest.mark.parametrize(
        "missing",
        ["aws_access_key_id", "aws_secret_access_key", "aws_region", "aws_service"],
    )
    def test_missing_field_raises(self, monkeypatch, missing):
        config = {k: v for k, v in self._CONFIG.items() if k != missing}
        with pytest.raises(ValueError, match=missing):
            self._sign(monkeypatch, config)


class TestCredentialStore:
    def test_get_returns_config(self, tmp_path, monkeypatch):
        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(
            json.dumps(
                {"credentials": {"callout:NC": {"auth_type": "Basic", "username": "u"}}}
            )
        )
        monkeypatch.setenv(CREDENTIAL_FILE_ENV_VAR, str(cred_file))
        store = CredentialStore()
        config = store.get("callout:NC")
        assert config["auth_type"] == "Basic"
        assert config["username"] == "u"

    def test_explicit_path_takes_precedence(self, tmp_path):
        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(
            json.dumps({"credentials": {"callout:NC": {"auth_type": "OAuth"}}})
        )
        store = CredentialStore(str(cred_file))
        assert store.get("callout:NC")["auth_type"] == "OAuth"

    def test_missing_file_raises(self, tmp_path):
        store = CredentialStore(str(tmp_path / "does-not-exist.json"))
        with pytest.raises(CredentialError):
            store.get("callout:NC")

    def test_missing_key_raises(self, tmp_path):
        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(
            json.dumps({"credentials": {"callout:Other": {"auth_type": "Basic"}}})
        )
        store = CredentialStore(str(cred_file))
        with pytest.raises(CredentialError):
            store.get("callout:NC")

    def test_missing_auth_type_raises(self, tmp_path):
        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(
            json.dumps({"credentials": {"callout:NC": {"username": "u"}}})
        )
        store = CredentialStore(str(cred_file))
        with pytest.raises(CredentialError):
            store.get("callout:NC")

    def test_non_object_json_raises(self, tmp_path):
        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(json.dumps(["not", "an", "object"]))
        store = CredentialStore(str(cred_file))
        with pytest.raises(CredentialError):
            store.get("callout:NC")


class _FakeToken:
    def __init__(self, access_token="tok", instance_url="https://org.example.com"):
        self.access_token = access_token
        self.instance_url = instance_url


class _FakeTokenProvider:
    def __init__(self, token=None):
        self._token = token or _FakeToken()

    def get_token(self):
        return self._token


class TestResolveBaseUrl:
    def test_prefers_connect_api_callout_url(self, monkeypatch):
        monkeypatch.setattr(
            "datacustomcode.named_credential.direct.url_resolver."
            "_callout_url_from_connect_api",
            lambda name, provider: "https://api.example.com/",
        )
        url = resolve_base_url(
            "callout:NC",
            {"target_url": "https://fallback.example.com"},
            _FakeTokenProvider(),
        )
        assert url == "https://api.example.com"

    def test_falls_back_to_target_url(self, monkeypatch):
        monkeypatch.setattr(
            "datacustomcode.named_credential.direct.url_resolver."
            "_callout_url_from_connect_api",
            lambda name, provider: None,
        )
        url = resolve_base_url(
            "callout:NC",
            {"target_url": "https://fallback.example.com/"},
            _FakeTokenProvider(),
        )
        assert url == "https://fallback.example.com"

    def test_no_url_available_raises(self, monkeypatch):
        monkeypatch.setattr(
            "datacustomcode.named_credential.direct.url_resolver."
            "_callout_url_from_connect_api",
            lambda name, provider: None,
        )
        with pytest.raises(CredentialError):
            resolve_base_url("callout:NC", {}, _FakeTokenProvider())

    def test_no_token_provider_uses_target_url(self):
        url = resolve_base_url(
            "callout:NC", {"target_url": "https://only.example.com"}, None
        )
        assert url == "https://only.example.com"


class TestDirectCalloutTransport:
    def _make_transport(self, tmp_path, monkeypatch, cred_entry):
        from datacustomcode.named_credential.direct import transport as transport_mod

        cred_file = tmp_path / "external_callout_config.json"
        cred_file.write_text(json.dumps({"credentials": {"callout:NC": cred_entry}}))
        monkeypatch.setattr(
            transport_mod.DirectCalloutTransport,
            "_build_token_provider",
            staticmethod(lambda profile, org: _FakeTokenProvider()),
        )
        monkeypatch.setattr(
            transport_mod,
            "resolve_base_url",
            lambda key, config, provider: "https://api.example.com",
        )
        return transport_mod.DirectCalloutTransport(credential_file=str(cred_file))

    def test_callout_happy_path(self, tmp_path, monkeypatch):
        from datacustomcode.named_credential.direct import transport as transport_mod

        transport = self._make_transport(
            tmp_path,
            monkeypatch,
            {"auth_type": "Custom", "custom_headers": {"X-Key": "v"}},
        )

        captured = {}

        class _Resp:
            def __init__(self):
                self.status_code = 200
                self.headers = {"Content-Type": "application/json"}
                self.text = '{"ok": true}'

        def fake_request(**kwargs):
            captured.update(kwargs)
            return _Resp()

        monkeypatch.setattr(transport_mod.requests, "request", fake_request)

        result = transport.callout(
            {
                "path": "callout:NC/search?q=sf",
                "method": "POST",
                "headers": {},
                "body": '{"a": 1}',
            }
        )

        # The query string embedded in the path is passed through verbatim.
        assert captured["url"] == "https://api.example.com/search?q=sf"
        assert captured["method"] == "POST"
        assert "params" not in captured
        assert captured["data"] == '{"a": 1}'
        # No Content-Type is assumed; headers are passed through verbatim.
        assert "Content-Type" not in captured["headers"]
        assert isinstance(captured["auth"], DynamicAuthHandler)

        assert result["status_code"] == 200
        assert result["body"] == '{"ok": true}'

    @pytest.mark.parametrize("method", ["PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
    def test_callout_forwards_non_get_post_methods(self, tmp_path, monkeypatch, method):
        from datacustomcode.named_credential.direct import transport as transport_mod

        transport = self._make_transport(tmp_path, monkeypatch, {"auth_type": "Custom"})

        captured = {}

        class _Resp:
            def __init__(self):
                self.status_code = 200
                self.headers = {}
                self.text = ""

        def fake_request(**kwargs):
            captured.update(kwargs)
            return _Resp()

        monkeypatch.setattr(transport_mod.requests, "request", fake_request)
        transport.callout({"path": "callout:NC/x", "method": method, "headers": {}})

        assert captured["method"] == method

    def test_callout_rejects_non_callout_url(self, tmp_path, monkeypatch):
        transport = self._make_transport(tmp_path, monkeypatch, {"auth_type": "Custom"})
        with pytest.raises(NamedCredentialCallError):
            transport.callout({"path": "https://example.com/x", "method": "GET"})

    @pytest.mark.parametrize("path", ["callout:", "callout:/path"])
    def test_callout_rejects_empty_named_credential(self, tmp_path, monkeypatch, path):
        transport = self._make_transport(tmp_path, monkeypatch, {"auth_type": "Custom"})
        with pytest.raises(NamedCredentialCallError):
            transport.callout({"path": path, "method": "GET", "headers": {}})

    def test_base_url_resolved_once_per_callout_key(self, tmp_path, monkeypatch):
        from datacustomcode.named_credential.direct import transport as transport_mod

        transport = self._make_transport(tmp_path, monkeypatch, {"auth_type": "Custom"})

        calls = {"count": 0}

        def counting_resolve(key, config, provider):
            calls["count"] += 1
            return "https://api.example.com"

        monkeypatch.setattr(transport_mod, "resolve_base_url", counting_resolve)

        class _Resp:
            status_code = 200
            headers: ClassVar[dict] = {}
            text = ""

        monkeypatch.setattr(transport_mod.requests, "request", lambda **kwargs: _Resp())

        # Many per-row callouts, same callout key: base URL is resolved just once.
        for _ in range(5):
            transport.callout({"path": "callout:NC/x", "method": "GET", "headers": {}})

        assert calls["count"] == 1
