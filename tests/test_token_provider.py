"""Tests for token_provider module."""

from unittest.mock import MagicMock, patch

import pytest

from datacustomcode.credentials import AuthType, Credentials
from datacustomcode.deploy import AccessTokenResponse
from datacustomcode.token_provider import CredentialsTokenProvider, SFCLITokenProvider


class TestCredentialsTokenProvider:
    """Tests for CredentialsTokenProvider."""

    def test_oauth_tokens_auth_type(self):
        """Test token retrieval with OAUTH_TOKENS auth type."""
        provider = CredentialsTokenProvider("test_profile")

        mock_credentials = Credentials(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
            refresh_token="test_refresh_token",
            auth_type=AuthType.OAUTH_TOKENS,
        )

        with patch(
            "datacustomcode.credentials.Credentials.from_available",
            return_value=mock_credentials,
        ):
            with patch("requests.post") as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = {
                    "access_token": "test_access_token",
                    "instance_url": "https://instance.salesforce.com",
                }
                mock_post.return_value = mock_response

                result = provider.get_token()

                assert isinstance(result, AccessTokenResponse)
                assert result.access_token == "test_access_token"
                assert result.instance_url == "https://instance.salesforce.com"

                # Verify correct auth type was used
                call_args = mock_post.call_args
                assert call_args[1]["data"]["grant_type"] == "refresh_token"
                assert call_args[1]["data"]["refresh_token"] == "test_refresh_token"

    def test_client_credentials_auth_type(self):
        """Test token retrieval with CLIENT_CREDENTIALS auth type."""
        provider = CredentialsTokenProvider("test_profile")

        mock_credentials = Credentials(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
            auth_type=AuthType.CLIENT_CREDENTIALS,
        )

        with patch(
            "datacustomcode.credentials.Credentials.from_available",
            return_value=mock_credentials,
        ):
            with patch("requests.post") as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = {
                    "access_token": "test_access_token",
                    "instance_url": "https://instance.salesforce.com",
                }
                mock_post.return_value = mock_response

                result = provider.get_token()

                assert isinstance(result, AccessTokenResponse)
                assert result.access_token == "test_access_token"
                assert result.instance_url == "https://instance.salesforce.com"

                # Verify correct auth type was used
                call_args = mock_post.call_args
                assert call_args[1]["data"]["grant_type"] == "client_credentials"
                assert "refresh_token" not in call_args[1]["data"]

    def test_unsupported_auth_type_raises_error(self):
        """Test that unsupported auth type raises RuntimeError."""
        provider = CredentialsTokenProvider("test_profile")

        # Create credentials with invalid auth_type
        mock_credentials = MagicMock()
        mock_credentials.login_url = "https://login.salesforce.com"
        mock_credentials.auth_type = "INVALID_AUTH_TYPE"

        with patch(
            "datacustomcode.credentials.Credentials.from_available",
            return_value=mock_credentials,
        ):
            with pytest.raises(RuntimeError, match="Unsupported auth_type"):
                provider.get_token()

    def test_missing_access_token_raises_error(self):
        """Test that missing access_token in response raises RuntimeError."""
        provider = CredentialsTokenProvider("test_profile")

        mock_credentials = Credentials(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
            refresh_token="test_refresh_token",
            auth_type=AuthType.OAUTH_TOKENS,
        )

        with patch(
            "datacustomcode.credentials.Credentials.from_available",
            return_value=mock_credentials,
        ):
            with patch("requests.post") as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = {
                    "instance_url": "https://instance.salesforce.com"
                }
                mock_post.return_value = mock_response

                with pytest.raises(
                    RuntimeError, match="missing access_token or instance_url"
                ):
                    provider.get_token()

    def test_request_exception_raises_runtime_error(self):
        """Test that request exceptions are wrapped in RuntimeError."""
        import requests

        provider = CredentialsTokenProvider("test_profile")

        mock_credentials = Credentials(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
            refresh_token="test_refresh_token",
            auth_type=AuthType.OAUTH_TOKENS,
        )

        with patch(
            "datacustomcode.credentials.Credentials.from_available",
            return_value=mock_credentials,
        ):
            with patch(
                "requests.post",
                side_effect=requests.exceptions.RequestException("Network error"),
            ):
                with pytest.raises(RuntimeError, match="Failed to get access token"):
                    provider.get_token()


class TestSFCLITokenProvider:
    """Tests for SFCLITokenProvider."""

    def test_successful_token_retrieval(self):
        """Test successful token retrieval from SF CLI."""
        import json

        provider = SFCLITokenProvider("test_org")

        cli_output = json.dumps(
            {
                "status": 0,
                "result": {
                    "accessToken": "cli_access_token",
                    "instanceUrl": "https://cli.salesforce.com",
                },
            }
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout=cli_output)

            result = provider.get_token()

            assert isinstance(result, AccessTokenResponse)
            assert result.access_token == "cli_access_token"
            assert result.instance_url == "https://cli.salesforce.com"

    def test_sf_command_not_found(self):
        """Test that FileNotFoundError is wrapped in RuntimeError."""
        provider = SFCLITokenProvider("test_org")

        with patch("subprocess.run", side_effect=FileNotFoundError()):
            with pytest.raises(RuntimeError, match="'sf' command was not found"):
                provider.get_token()
