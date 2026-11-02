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

import sys
from unittest.mock import MagicMock, patch

from pydantic import ValidationError
import pytest

from datacustomcode.named_credential.default import DefaultNamedCredential
from datacustomcode.named_credential.types.http_method import HTTPMethod
from datacustomcode.named_credential.types.http_request import HTTPRequest
from datacustomcode.named_credential.types.http_request_builder import (
    HTTPRequestBuilder,
)
from datacustomcode.named_credential.types.http_response import HTTPResponse
from datacustomcode.named_credential.types.http_response_builder import (
    HTTPResponseBuilder,
)


class TestHTTPRequest:
    def test_url_required(self):
        with pytest.raises(ValidationError):
            HTTPRequest()

    def test_url_min_length(self):
        with pytest.raises(ValidationError):
            HTTPRequest(url="")

    def test_method_defaults_to_get(self):
        request = HTTPRequest(url="callout:NC/path")
        assert request.method == "GET"

    def test_headers_default_empty(self):
        request = HTTPRequest(url="callout:NC/path")
        assert request.headers == {}

    def test_method_accepts_local_enum(self):
        request = HTTPRequest(url="callout:NC/path", method=HTTPMethod.POST)
        assert request.method == "POST"

    def test_method_normalizes_lowercase_string(self):
        request = HTTPRequest(url="callout:NC/path", method="post")
        assert request.method == "POST"

    @pytest.mark.skipif(
        sys.version_info < (3, 11), reason="http.HTTPMethod added in 3.11"
    )
    def test_method_accepts_stdlib_httpmethod(self):
        from http import HTTPMethod as StdHTTPMethod

        request = HTTPRequest(url="callout:NC/path", method=StdHTTPMethod.GET)
        assert request.method == "GET"

    @pytest.mark.parametrize("method", ["PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
    def test_non_get_post_methods_accepted(self, method):
        request = HTTPRequest(url="callout:NC/path", method=method)
        assert request.method == method

    def test_method_accepts_local_enum_put(self):
        request = HTTPRequest(url="callout:NC/path", method=HTTPMethod.PUT)
        assert request.method == "PUT"

    def test_unsupported_method_rejected(self):
        with pytest.raises(ValidationError):
            HTTPRequest(url="callout:NC/path", method="TRACE")

    def test_query_string_in_url_preserved(self):
        request = HTTPRequest(url="callout:NC/v1/accounts?limit=10&active=true")
        assert request.url == "callout:NC/v1/accounts?limit=10&active=true"


class TestHTTPRequestBuilder:
    def test_builder_basic_usage(self):
        request = (
            HTTPRequestBuilder()
            .set_url("callout:Nominatim_Geocoding/search?q=sf&format=json")
            .set_method(HTTPMethod.GET)
            .set_headers({"Accept": "application/json"})
            .build()
        )
        assert request.url == "callout:Nominatim_Geocoding/search?q=sf&format=json"
        assert request.method == "GET"
        assert request.headers == {"Accept": "application/json"}

    def test_builder_default_method_is_get(self):
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        assert request.method == "GET"

    def test_builder_accepts_string_method(self):
        request = (
            HTTPRequestBuilder().set_url("callout:NC/path").set_method("post").build()
        )
        assert request.method == "POST"

    def test_builder_validates_on_build(self):
        with pytest.raises(ValidationError):
            HTTPRequestBuilder().set_url("").build()

    def test_builder_sets_response_timeout_header(self):
        request = (
            HTTPRequestBuilder()
            .set_url("callout:NC/path")
            .set_headers({"Accept": "application/json"})
            .set_response_timeout_seconds(60)
            .build()
        )
        assert request.headers == {
            "Accept": "application/json",
            "ctx-callout-response-timeout-seconds": "60",
        }

    def test_builder_response_timeout_is_order_independent(self):
        # Setting the timeout before set_headers must not be wiped out by the
        # header map replacement — build() applies it on top either way.
        request = (
            HTTPRequestBuilder()
            .set_url("callout:NC/path")
            .set_response_timeout_seconds(60)
            .set_headers({"Accept": "application/json"})
            .build()
        )
        assert request.headers == {
            "Accept": "application/json",
            "ctx-callout-response-timeout-seconds": "60",
        }

    @pytest.mark.parametrize("bad", [0, -5, True, 1.5, "30", None])
    def test_builder_rejects_invalid_response_timeout(self, bad):
        with pytest.raises(ValueError):
            HTTPRequestBuilder().set_url(
                "callout:NC/path"
            ).set_response_timeout_seconds(bad)


class TestHTTPResponse:
    def test_response_defaults(self):
        response = HTTPResponse(status_code=200)
        assert response.headers == {}
        assert response.body == ""

    def test_is_success_for_2xx(self):
        assert HTTPResponse(status_code=200).is_success is True
        assert HTTPResponse(status_code=204).is_success is True
        assert HTTPResponse(status_code=299).is_success is True

    def test_is_error_for_non_2xx(self):
        assert HTTPResponse(status_code=404).is_success is False
        assert HTTPResponse(status_code=404).is_error is True
        assert HTTPResponse(status_code=500).is_error is True

    def test_status_code_validation(self):
        with pytest.raises(ValidationError):
            HTTPResponse(status_code=-1)


class TestHTTPResponseBuilder:
    def test_build_from_dict(self):
        response = HTTPResponseBuilder.build(
            {"status_code": 200, "headers": {"X": "y"}, "body": '{"ok": true}'}
        )
        assert isinstance(response, HTTPResponse)
        assert response.status_code == 200
        assert response.headers == {"X": "y"}
        assert response.body == '{"ok": true}'

    def test_build_requires_status_code(self):
        with pytest.raises(ValidationError):
            HTTPResponseBuilder.build({"headers": {}})


class TestDefaultNamedCredential:
    def test_callout_delegates_to_transport(self, monkeypatch):
        nc = DefaultNamedCredential()

        class _FakeTransport:
            def callout(self, callout_request):
                return {"status_code": 200, "headers": {}, "body": "{}"}

        monkeypatch.setattr(nc, "_get_transport", lambda: _FakeTransport())
        result = nc._callout({"path": "callout:NC/x", "method": "GET"})
        assert result["status_code"] == 200

    def test_request_forwards_raw_body_and_response(self, monkeypatch):
        nc = DefaultNamedCredential()
        captured = {}

        def fake_callout(callout_request):
            captured.update(callout_request)
            return {
                "status_code": 200,
                "headers": {"Content-Type": "application/json"},
                "body": '{"result": "ok"}',
            }

        monkeypatch.setattr(nc, "_callout", fake_callout)
        request = (
            HTTPRequestBuilder()
            .set_url("callout:NC/search?q=sf")
            .set_method(HTTPMethod.POST)
            .set_headers({"Accept": "application/json"})
            .build()
        )
        response = nc.request(request, '{"key": "value"}')

        # The query string travels inside the path verbatim.
        assert captured["path"] == "callout:NC/search?q=sf"
        assert captured["method"] == "POST"
        assert captured["headers"] == {"Accept": "application/json"}
        assert "query_params" not in captured
        assert captured["body"] == '{"key": "value"}'

        assert response.status_code == 200
        assert response.headers == {"Content-Type": "application/json"}
        assert response.body == '{"result": "ok"}'
        assert response.is_success is True

    def test_request_without_body_sends_empty_string(self, monkeypatch):
        nc = DefaultNamedCredential()
        captured = {}

        def fake_callout(callout_request):
            captured.update(callout_request)
            return {"status_code": 204, "headers": {}, "body": ""}

        monkeypatch.setattr(nc, "_callout", fake_callout)
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        response = nc.request(request)

        assert captured["body"] == ""
        assert response.status_code == 204
        assert response.body == ""

    def test_request_non_json_body_returned_verbatim(self, monkeypatch):
        nc = DefaultNamedCredential()

        def fake_callout(callout_request):
            return {"status_code": 200, "headers": {}, "body": "plain text"}

        monkeypatch.setattr(nc, "_callout", fake_callout)
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        response = nc.request(request)
        assert response.body == "plain text"

    def test_request_json_array_body_returned_verbatim(self, monkeypatch):
        nc = DefaultNamedCredential()

        def fake_callout(callout_request):
            return {"status_code": 200, "headers": {}, "body": "[1, 2, 3]"}

        monkeypatch.setattr(nc, "_callout", fake_callout)
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        response = nc.request(request)
        assert response.body == "[1, 2, 3]"

    def test_request_opaque_string_body_sent_verbatim(self, monkeypatch):
        nc = DefaultNamedCredential()
        captured = {}

        def fake_callout(callout_request):
            captured.update(callout_request)
            return {"status_code": 200, "headers": {}, "body": ""}

        monkeypatch.setattr(nc, "_callout", fake_callout)
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        nc.request(request, "a=1&b=2")

        assert captured["body"] == "a=1&b=2"


class TestDefaultSparkNamedCredential:
    def test_delegates_to_underlying(self):
        from datacustomcode.named_credential.spark_default import (
            DefaultSparkNamedCredential,
        )

        sentinel = object()

        class _Underlying:
            def __init__(self):
                self.calls = []

            def request(self, request, body=None):
                self.calls.append((request, body))
                return sentinel

        underlying = _Underlying()
        spark_nc = DefaultSparkNamedCredential(named_credential=underlying)

        request = HTTPRequestBuilder().set_url("callout:NC/path").build()
        result = spark_nc.request(request, '{"k": "v"}')

        assert result is sentinel
        assert underlying.calls == [(request, '{"k": "v"}')]

    def test_builds_underlying_from_config_when_absent(self, monkeypatch):
        from datacustomcode.named_credential import spark_default

        built = object()
        monkeypatch.setattr(
            spark_default, "_build_underlying_named_credential", lambda: built
        )
        spark_nc = spark_default.DefaultSparkNamedCredential()
        assert spark_nc._named_credential is built

    def test_config_resolves_to_spark_default(self):
        from datacustomcode.named_credential.spark_base import SparkNamedCredential
        from datacustomcode.named_credential.spark_default import (
            DefaultSparkNamedCredential,
        )

        resolved = SparkNamedCredential.subclass_from_config_name(
            "DefaultSparkNamedCredential"
        )
        assert resolved is DefaultSparkNamedCredential


class TestDefaultSparkNamedCredentialRequestCol:
    """The client-side per-row UDF path used during development."""

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.lit")
    def test_wraps_callout_in_udf_over_body_column(self, mock_lit, mock_udf):
        from datacustomcode.named_credential.spark_default import (
            DefaultSparkNamedCredential,
        )

        sentinel_udf = MagicMock(name="udf")
        sentinel_applied = MagicMock(name="udf_applied")
        sentinel_udf.return_value = sentinel_applied
        mock_udf.return_value = sentinel_udf

        underlying = MagicMock()
        underlying.request.return_value = HTTPResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body='{"ok":true}',
        )
        spark_nc = DefaultSparkNamedCredential(named_credential=underlying)

        request = HTTPRequestBuilder().set_url("callout:NC/v1/accounts").build()
        body_col = MagicMock(name="body_col")
        result = spark_nc.request_col(request, body_col)

        assert result is sentinel_applied
        mock_udf.assert_called_once()
        # The caller's body column is what the UDF is applied to.
        sentinel_udf.assert_called_once_with(body_col)
        mock_lit.assert_not_called()

        # Exercise the wrapped callout function on a row body.
        udf_fn = mock_udf.call_args.args[0]
        out = udf_fn('{"name": "acme"}')

        assert out["status"] == "SUCCESS"
        assert out["error_code"] is None
        assert out["error_message"] is None
        # response mirrors the production UDF: a struct of status_code/body/headers
        payload = out["response"]
        assert payload["status_code"] == 200
        assert payload["headers"] == {"Content-Type": "application/json"}
        # Body is forwarded verbatim, never re-parsed/re-serialized.
        assert payload["body"] == '{"ok":true}'

        # The raw body string (NOT a parsed dict) is forwarded to the callout.
        sent_request, sent_body = underlying.request.call_args.args
        assert sent_request is request
        assert sent_body == '{"name": "acme"}'

    @patch("pyspark.sql.functions.udf")
    @patch("pyspark.sql.functions.lit")
    def test_defaults_body_to_typed_null_column(self, mock_lit, mock_udf):
        from datacustomcode.named_credential.spark_default import (
            DefaultSparkNamedCredential,
        )

        null_col = MagicMock(name="null_col")
        lit_none = MagicMock(name="lit_none")
        lit_none.cast.return_value = null_col
        mock_lit.return_value = lit_none

        sentinel_udf = MagicMock(name="udf")
        mock_udf.return_value = sentinel_udf

        spark_nc = DefaultSparkNamedCredential(named_credential=MagicMock())
        request = HTTPRequestBuilder().set_url("callout:NC/status").build()
        spark_nc.request_col(request)

        # With no body column, a typed null string column is applied instead.
        sentinel_udf.assert_called_once_with(null_col)


class TestInvokeCalloutAsStruct:
    """The callout-to-struct shaping shared by every row."""

    def test_success_struct_carries_response_fields(self):
        from datacustomcode.named_credential.spark_default import (
            _invoke_callout_as_struct,
        )

        underlying = MagicMock()
        underlying.request.return_value = HTTPResponse(
            status_code=201,
            headers={"X-Trace": "abc"},
            body="[1,2,3]",
        )
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()

        out = _invoke_callout_as_struct(underlying, request, '{"a": 1}')

        assert out["status"] == "SUCCESS"
        # response is a typed struct mirroring the runtime UDF's fields.
        assert out["response"] == {
            "status_code": 201,
            "body": "[1,2,3]",
            "headers": {"X-Trace": "abc"},
        }
        # The request body is forwarded verbatim (never validated as JSON).
        assert underlying.request.call_args.args[1] == '{"a": 1}'

    def test_non_json_request_body_is_forwarded_not_rejected(self):
        from datacustomcode.named_credential.spark_default import (
            _invoke_callout_as_struct,
        )

        # Mirrors the runtime: an opaque body (form-encoded / plain text) is sent
        # as-is instead of being rejected the way JSON parsing would.
        underlying = MagicMock()
        underlying.request.return_value = HTTPResponse(
            status_code=200,
            headers={},
            body="OK",
        )
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()

        out = _invoke_callout_as_struct(underlying, request, "a=1&b=2")

        assert out["status"] == "SUCCESS"
        assert underlying.request.call_args.args[1] == "a=1&b=2"
        # A non-JSON response body is preserved verbatim, not dropped to "".
        assert out["response"]["body"] == "OK"

    def test_none_body_forwards_none_and_keeps_all_keys(self):
        from datacustomcode.named_credential.spark_default import (
            _invoke_callout_as_struct,
        )

        underlying = MagicMock()
        underlying.request.return_value = HTTPResponse(
            status_code=200,
            headers={},
            body="",
        )
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()

        out = _invoke_callout_as_struct(underlying, request, None)

        assert out["status"] == "SUCCESS"
        # Empty headers/body keep their keys, matching the runtime UDF's
        # includingDefaultValueFields defaults.
        assert out["response"] == {
            "status_code": 200,
            "body": "",
            "headers": {},
        }
        # A null body column forwards None (not "null") to the callout.
        assert underlying.request.call_args.args[1] is None

    def test_transport_error_yields_error_struct(self):
        from datacustomcode.named_credential.spark_default import (
            _invoke_callout_as_struct,
        )

        underlying = MagicMock()
        underlying.request.side_effect = RuntimeError("proxy down")
        request = HTTPRequestBuilder().set_url("callout:NC/path").build()

        out = _invoke_callout_as_struct(underlying, request, '{"a": 1}')

        assert out["status"] == "ERROR"
        assert out["response"] is None
        assert out["error_message"] == "proxy down"
