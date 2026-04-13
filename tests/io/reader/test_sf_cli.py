from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from datacustomcode.io.reader.sf_cli import API_VERSION, SFCLIDataCloudReader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_reader(
    sf_cli_org: str = "dev1",
    dataspace: str | None = None,
    default_row_limit: int | None = 1000,
) -> SFCLIDataCloudReader:
    spark = MagicMock()
    spark.createDataFrame.return_value = MagicMock()
    return SFCLIDataCloudReader(
        spark=spark,
        sf_cli_org=sf_cli_org,
        dataspace=dataspace,
        default_row_limit=default_row_limit,
    )


def _sf_display_output(
    access_token: str = "tok", instance_url: str = "https://example.my.salesforce.com"
) -> str:
    return json.dumps(
        {
            "status": 0,
            "result": {"accessToken": access_token, "instanceUrl": instance_url},
        }
    )


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestSFCLIDataCloudReaderInit:
    def test_stores_org(self):
        reader = _make_reader(sf_cli_org="my-org")
        assert reader.sf_cli_org == "my-org"

    def test_dataspace_none_becomes_default(self):
        reader = _make_reader(dataspace=None)
        assert reader.dataspace == "default"

    def test_dataspace_string_default_stays_default(self):
        reader = _make_reader(dataspace="default")
        assert reader.dataspace == "default"

    def test_custom_dataspace_preserved(self):
        reader = _make_reader(dataspace="myspace")
        assert reader.dataspace == "myspace"


# ---------------------------------------------------------------------------
# _get_token
# ---------------------------------------------------------------------------


class TestGetToken:
    @pytest.fixture
    def reader(self):
        return _make_reader()

    def _run_result(self, stdout: str) -> MagicMock:
        result = MagicMock()
        result.stdout = stdout
        return result

    def test_returns_token_and_instance_url(self, reader):
        with patch(
            "subprocess.run",
            return_value=self._run_result(
                _sf_display_output("mytoken", "https://org.salesforce.com")
            ),
        ) as mock_run:
            token, url = reader._get_token()

        assert token == "mytoken"
        assert url == "https://org.salesforce.com"
        mock_run.assert_called_once_with(
            ["sf", "org", "display", "--target-org", "dev1", "--json"],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

    def test_file_not_found_raises_runtime_error(self, reader):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(RuntimeError, match="'sf' command was not found"):
                reader._get_token()

    def test_timeout_raises_runtime_error(self, reader):
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="sf", timeout=30),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                reader._get_token()

    def test_called_process_error_raises_runtime_error(self, reader):
        exc = subprocess.CalledProcessError(returncode=1, cmd="sf", stderr="auth error")
        with patch("subprocess.run", side_effect=exc):
            with pytest.raises(RuntimeError, match="failed for org"):
                reader._get_token()

    def test_called_process_error_includes_stderr(self, reader):
        exc = subprocess.CalledProcessError(
            returncode=1, cmd="sf", stderr="not authenticated"
        )
        with patch("subprocess.run", side_effect=exc):
            with pytest.raises(RuntimeError, match="not authenticated"):
                reader._get_token()

    def test_invalid_json_raises_runtime_error(self, reader):
        result = MagicMock()
        result.stdout = "not valid json{"
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="Failed to parse"):
                reader._get_token()

    def test_nonzero_status_raises_runtime_error(self, reader):
        payload = json.dumps({"status": 1, "message": "something went wrong"})
        result = MagicMock()
        result.stdout = payload
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="something went wrong"):
                reader._get_token()

    def test_nonzero_status_without_message_uses_unknown_error(self, reader):
        payload = json.dumps({"status": 1})
        result = MagicMock()
        result.stdout = payload
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="unknown error"):
                reader._get_token()

    def test_missing_access_token_raises_runtime_error(self, reader):
        payload = json.dumps(
            {"status": 0, "result": {"instanceUrl": "https://x.salesforce.com"}}
        )
        result = MagicMock()
        result.stdout = payload
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="access token or instance URL"):
                reader._get_token()

    def test_missing_instance_url_raises_runtime_error(self, reader):
        payload = json.dumps({"status": 0, "result": {"accessToken": "tok"}})
        result = MagicMock()
        result.stdout = payload
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="access token or instance URL"):
                reader._get_token()


# ---------------------------------------------------------------------------
# _execute_query
# ---------------------------------------------------------------------------


class TestExecuteQuery:
    @pytest.fixture
    def reader(self):
        return _make_reader()

    @pytest.fixture
    def mock_token(self, reader):
        with patch.object(
            reader, "_get_token", return_value=("mytoken", "https://org.salesforce.com")
        ):
            yield

    def _mock_response(
        self, status_code: int = 200, json_body: dict | None = None, text: str = ""
    ) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.text = text
        response.json.return_value = json_body or {}
        return response

    def test_posts_to_correct_url(self, reader, mock_token):
        api_response = {"metadata": [{"name": "col"}], "data": [["v"]]}
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", 100)

        url = mock_post.call_args[0][0]
        assert (
            url
            == f"https://org.salesforce.com/services/data/{API_VERSION}/ssot/query-sql"
        )

    def test_passes_bearer_token_header(self, reader, mock_token):
        api_response = {"metadata": [], "data": []}
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", 10)

        headers = mock_post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer mytoken"

    def test_passes_dataspace_param(self, reader, mock_token):
        api_response = {"metadata": [], "data": []}
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", 10)

        params = mock_post.call_args.kwargs["params"]
        assert params["dataspace"] == "default"

    def test_appends_limit_to_sql(self, reader, mock_token):
        api_response = {"metadata": [], "data": []}
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", 42)

        body = mock_post.call_args.kwargs["json"]
        assert body["sql"] == "SELECT * FROM foo LIMIT 42"

    def test_returns_dataframe_with_rows(self, reader, mock_token):
        api_response = {
            "metadata": [{"name": "id"}, {"name": "name"}],
            "data": [[1, "alice"], [2, "bob"]],
        }
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ):
            df = reader._execute_query("SELECT * FROM foo", 100)

        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2

    def test_returns_empty_dataframe_when_no_rows(self, reader, mock_token):
        api_response = {"metadata": [{"name": "id"}, {"name": "name"}], "data": []}
        with patch(
            "requests.post", return_value=self._mock_response(json_body=api_response)
        ):
            df = reader._execute_query("SELECT * FROM foo", 100)

        assert list(df.columns) == ["id", "name"]
        assert len(df) == 0

    def test_http_error_raises_runtime_error(self, reader, mock_token):
        with patch(
            "requests.post",
            return_value=self._mock_response(status_code=401, text="Unauthorized"),
        ):
            with pytest.raises(RuntimeError, match="HTTP 401"):
                reader._execute_query("SELECT * FROM foo", 10)

    def test_http_error_uses_json_message_when_available(self, reader, mock_token):
        error_body = [{"message": "insufficient privileges"}]
        response = self._mock_response(status_code=403, text="Forbidden")
        response.json.return_value = error_body
        with patch("requests.post", return_value=response):
            with pytest.raises(RuntimeError, match="insufficient privileges"):
                reader._execute_query("SELECT * FROM foo", 10)

    def test_http_error_falls_back_to_text_when_json_not_list(self, reader, mock_token):
        response = self._mock_response(status_code=500, text="Internal Server Error")
        response.json.return_value = {"error": "oops"}  # dict, not list
        with patch("requests.post", return_value=response):
            with pytest.raises(RuntimeError, match="Internal Server Error"):
                reader._execute_query("SELECT * FROM foo", 10)

    def test_request_exception_raises_runtime_error(self, reader, mock_token):
        import requests as req_lib

        with patch(
            "requests.post", side_effect=req_lib.RequestException("connection refused")
        ):
            with pytest.raises(RuntimeError, match="Data Cloud query request failed"):
                reader._execute_query("SELECT * FROM foo", 10)

    def test_custom_dataspace_passed_as_param(self):
        reader = _make_reader(dataspace="myspace")
        with patch.object(
            reader, "_get_token", return_value=("tok", "https://org.salesforce.com")
        ):
            api_response = {"metadata": [], "data": []}
            with patch(
                "requests.post",
                return_value=self._mock_response(json_body=api_response),
            ) as mock_post:
                reader._execute_query("SELECT * FROM foo", 10)

        params = mock_post.call_args.kwargs["params"]
        assert params["dataspace"] == "myspace"


# ---------------------------------------------------------------------------
# read_dlo / read_dmo
# ---------------------------------------------------------------------------


class TestReadDloAndDmo:
    @pytest.fixture
    def reader(self):
        return _make_reader()

    @pytest.fixture
    def sample_df(self):
        """DataFrame with PascalCase columns, as the REST API metadata returns."""
        return pd.DataFrame({"Id__c": [1, 2], "Name__c": ["a", "b"]})

    @pytest.mark.parametrize(
        "method,obj_name",
        [
            ("read_dlo", "MyDLO__dll"),
            ("read_dmo", "MyDMO__dlm"),
        ],
    )
    def test_executes_select_star_query(self, reader, sample_df, method, obj_name):
        with patch.object(
            reader, "_execute_query", return_value=sample_df
        ) as mock_exec:
            getattr(reader, method)(obj_name)

        mock_exec.assert_called_once_with(f"SELECT * FROM {obj_name}", None)

    @pytest.mark.parametrize("method", ["read_dlo", "read_dmo"])
    def test_custom_row_limit(self, reader, sample_df, method):
        with patch.object(
            reader, "_execute_query", return_value=sample_df
        ) as mock_exec:
            getattr(reader, method)("SomeObj", row_limit=50)

        _, row_limit_arg = mock_exec.call_args[0]
        assert row_limit_arg == 50

    @pytest.mark.parametrize("method", ["read_dlo", "read_dmo"])
    def test_auto_infers_schema_when_none_given(self, reader, sample_df, method):
        from pyspark.sql.types import StructType

        with patch.object(reader, "_execute_query", return_value=sample_df):
            getattr(reader, method)("SomeObj")

        _, schema_arg = reader.spark.createDataFrame.call_args[0]
        assert isinstance(schema_arg, StructType)

    @pytest.mark.parametrize("method", ["read_dlo", "read_dmo"])
    def test_auto_infers_schema_lowercases_pascal_case_columns(
        self, reader, sample_df, method
    ):
        """Schema is lowercased so local results match Data Cloud column names."""
        with patch.object(reader, "_execute_query", return_value=sample_df):
            getattr(reader, method)("SomeObj")

        _, schema_arg = reader.spark.createDataFrame.call_args[0]
        assert all(f.name == f.name.lower() for f in schema_arg.fields)

    @pytest.mark.parametrize("method", ["read_dlo", "read_dmo"])
    def test_uses_provided_schema(self, reader, sample_df, method):
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        custom_schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
            ]
        )

        with patch.object(reader, "_execute_query", return_value=sample_df):
            getattr(reader, method)("SomeObj", schema=custom_schema)

        _, schema_arg = reader.spark.createDataFrame.call_args[0]
        assert schema_arg is custom_schema

    @pytest.mark.parametrize("method", ["read_dlo", "read_dmo"])
    def test_returns_spark_dataframe(self, reader, sample_df, method):
        expected = MagicMock()
        reader.spark.createDataFrame.return_value = expected

        with patch.object(reader, "_execute_query", return_value=sample_df):
            result = getattr(reader, method)("SomeObj")

        assert result is expected


# ---------------------------------------------------------------------------
# No default row limit (deployed environment)
# ---------------------------------------------------------------------------


class TestSFCLINoDefaultRowLimit:
    """Tests for deployed behavior where default_row_limit is None."""

    @pytest.fixture
    def reader(self):
        return _make_reader(default_row_limit=None)

    @pytest.fixture
    def mock_token(self, reader):
        with patch.object(
            reader, "_get_token", return_value=("tok", "https://org.salesforce.com")
        ):
            yield

    def _mock_response(
        self, status_code: int = 200, json_body: dict | None = None, text: str = ""
    ) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.text = text
        response.json.return_value = json_body or {}
        return response

    def test_execute_query_omits_limit_when_no_default(self, reader, mock_token):
        """When default_row_limit is None and row_limit is None, no LIMIT clause."""
        api_response = {"metadata": [{"name": "col"}], "data": [["v"]]}
        with patch(
            "requests.post",
            return_value=self._mock_response(json_body=api_response),
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", None)

        body = mock_post.call_args.kwargs["json"]
        assert body["sql"] == "SELECT * FROM foo"

    def test_execute_query_applies_explicit_limit_when_no_default(
        self, reader, mock_token
    ):
        """An explicit row_limit is always applied, even without a default."""
        api_response = {"metadata": [{"name": "col"}], "data": [["v"]]}
        with patch(
            "requests.post",
            return_value=self._mock_response(json_body=api_response),
        ) as mock_post:
            reader._execute_query("SELECT * FROM foo", 42)

        body = mock_post.call_args.kwargs["json"]
        assert body["sql"] == "SELECT * FROM foo LIMIT 42"
