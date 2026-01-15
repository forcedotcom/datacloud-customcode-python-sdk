from __future__ import annotations

import configparser
import os
from unittest.mock import mock_open, patch

import pytest

from datacustomcode.credentials import AuthType, Credentials


class TestCredentials:
    """Test suite for Credentials class supporting multiple auth types."""

    # ============== OAuth Tokens Tests (Default) ==============

    def test_from_env_oauth_tokens_default(self):
        """Test loading OAuth Tokens credentials from env vars (default)."""
        env_vars = {
            "SFDC_LOGIN_URL": "https://test.login.url",
            "SFDC_CLIENT_ID": "test_client_id",
            "SFDC_CLIENT_SECRET": "test_secret",
            "SFDC_REFRESH_TOKEN": "test_refresh_token",
            "SFDC_CORE_TOKEN": "test_core_token",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            creds = Credentials.from_env()

            assert creds.auth_type == AuthType.OAUTH_TOKENS
            assert creds.client_secret == "test_secret"
            assert creds.refresh_token == "test_refresh_token"
            assert creds.core_token == "test_core_token"
            assert creds.client_id == "test_client_id"
            assert creds.login_url == "https://test.login.url"

    def test_from_env_oauth_tokens_explicit(self):
        """Test loading OAuth Tokens credentials with explicit auth type."""
        env_vars = {
            "SFDC_LOGIN_URL": "https://test.login.url",
            "SFDC_CLIENT_ID": "test_client_id",
            "SFDC_AUTH_TYPE": "oauth_tokens",
            "SFDC_CLIENT_SECRET": "test_secret",
            "SFDC_REFRESH_TOKEN": "test_refresh_token",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            creds = Credentials.from_env()

            assert creds.auth_type == AuthType.OAUTH_TOKENS
            assert creds.client_secret == "test_secret"
            assert creds.refresh_token == "test_refresh_token"

    def test_from_ini_oauth_tokens(self):
        """Test loading OAuth Tokens credentials from an INI file."""
        ini_content = """
        [oauth_profile]
        auth_type = oauth_tokens
        login_url = https://oauth.login.url
        client_id = oauth_client_id
        client_secret = oauth_secret
        refresh_token = oauth_refresh_token
        core_token = oauth_core_token
        """

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds = Credentials.from_ini(
                    profile="oauth_profile", ini_file="fake_path"
                )
                assert creds.auth_type == AuthType.OAUTH_TOKENS
                assert creds.client_secret == "oauth_secret"
                assert creds.refresh_token == "oauth_refresh_token"
                assert creds.core_token == "oauth_core_token"
                assert creds.client_id == "oauth_client_id"

    def test_from_ini_default_auth_type(self):
        """Test that INI files without auth_type default to oauth_tokens."""
        ini_content = """
        [default]
        login_url = https://ini.login.url
        client_id = ini_client_id
        client_secret = ini_secret
        refresh_token = ini_refresh_token
        """

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds = Credentials.from_ini(profile="default", ini_file="fake_path")
                assert creds.auth_type == AuthType.OAUTH_TOKENS
                assert creds.client_secret == "ini_secret"
                assert creds.refresh_token == "ini_refresh_token"

    def test_from_ini_client_credentials(self):
        """Test loading client credentials auth from an INI file."""
        ini_content = """
        [default]
        auth_type = client_credentials
        login_url = https://ini.login.url
        client_id = ini_client_id
        client_secret = ini_secret
        """

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds = Credentials.from_ini(profile="default", ini_file="fake_path")
                assert creds.auth_type == AuthType.CLIENT_CREDENTIALS
                assert creds.client_id == "ini_client_id"
                assert creds.client_secret == "ini_secret"
                assert creds.login_url == "https://ini.login.url"

    def test_oauth_tokens_missing_refresh_token(self):
        """Test that OAuth Tokens auth requires refresh token."""
        with pytest.raises(ValueError, match="refresh_token"):
            Credentials(
                login_url="https://test.login.url",
                client_id="test_client_id",
                auth_type=AuthType.OAUTH_TOKENS,
                client_secret="test_secret",
            )

    def test_oauth_tokens_missing_client_secret(self):
        """Test that OAuth Tokens auth requires client secret."""
        with pytest.raises(ValueError, match="client_secret"):
            Credentials(
                login_url="https://test.login.url",
                client_id="test_client_id",
                auth_type=AuthType.OAUTH_TOKENS,
                refresh_token="test_refresh_token",
            )

    def test_from_env_missing_vars(self):
        """Test that missing environment variables raise appropriate error."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="SFDC_LOGIN_URL and SFDC_CLIENT_ID"):
                Credentials.from_env()

    def test_from_env_client_credentials(self):
        """Test loading client credentials auth from environment variables."""
        env_vars = {
            "SFDC_LOGIN_URL": "https://test.login.url",
            "SFDC_CLIENT_ID": "test_client_id",
            "SFDC_AUTH_TYPE": "client_credentials",
            "SFDC_CLIENT_SECRET": "test_secret",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            creds = Credentials.from_env()

            assert creds.auth_type == AuthType.CLIENT_CREDENTIALS
            assert creds.client_id == "test_client_id"
            assert creds.client_secret == "test_secret"

    # ============== from_available Tests ==============

    def test_from_available_env(self):
        """Test that from_available uses environment variables when available."""
        env_vars = {
            "SFDC_LOGIN_URL": "https://test.login.url",
            "SFDC_CLIENT_ID": "test_client_id",
            "SFDC_CLIENT_SECRET": "test_secret",
            "SFDC_REFRESH_TOKEN": "test_refresh_token",
        }

        with (
            patch.dict(os.environ, env_vars, clear=True),
            patch("os.path.exists", return_value=False),
        ):
            creds = Credentials.from_available()

            assert creds.auth_type == AuthType.OAUTH_TOKENS
            assert creds.client_id == "test_client_id"
            assert creds.client_secret == "test_secret"
            assert creds.refresh_token == "test_refresh_token"
            assert creds.login_url == "https://test.login.url"

    def test_from_available_ini(self):
        """Test that from_available uses INI file when env vars not available."""
        ini_content = """
        [default]
        auth_type = oauth_tokens
        login_url = https://ini.login.url
        client_id = ini_client_id
        client_secret = ini_secret
        refresh_token = ini_refresh_token
        """

        with (
            patch.dict(os.environ, {}, clear=True),
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds = Credentials.from_available()

                assert creds.auth_type == AuthType.OAUTH_TOKENS
                assert creds.client_id == "ini_client_id"
                assert creds.client_secret == "ini_secret"
                assert creds.refresh_token == "ini_refresh_token"
                assert creds.login_url == "https://ini.login.url"

    def test_from_available_no_creds(self):
        """Test that from_available raises error when no credentials are found."""
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("os.path.exists", return_value=False),
        ):
            with pytest.raises(ValueError, match="Credentials not found"):
                Credentials.from_available()

    # ============== update_ini Tests ==============

    def test_update_ini_oauth_tokens(self):
        """Test updating OAuth Tokens credentials in an INI file."""
        ini_content = """
        [default]
        auth_type = oauth_tokens
        login_url = https://old.login.url
        client_id = old_client_id
        client_secret = old_secret
        refresh_token = old_refresh_token
        """

        creds = Credentials(
            login_url="https://new.login.url",
            client_id="new_client_id",
            auth_type=AuthType.OAUTH_TOKENS,
            client_secret="new_secret",
            refresh_token="new_refresh_token",
            core_token="new_core_token",
        )

        mock_file = mock_open(read_data=ini_content)

        with (
            patch("os.path.expanduser", return_value="/fake/expanded/path"),
            patch("os.path.exists", return_value=True),
            patch("os.makedirs"),
            patch("builtins.open", mock_file),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds.update_ini(profile="default", ini_file="~/fake_path")

                mock_file.assert_called_with("/fake/expanded/path", "w")
                assert mock_config["default"]["auth_type"] == "oauth_tokens"
                assert mock_config["default"]["client_id"] == "new_client_id"
                assert mock_config["default"]["client_secret"] == "new_secret"
                assert mock_config["default"]["refresh_token"] == "new_refresh_token"
                assert mock_config["default"]["core_token"] == "new_core_token"
                assert mock_config["default"]["login_url"] == "https://new.login.url"

    def test_update_ini_client_credentials(self):
        """Test updating client credentials auth in an INI file."""
        ini_content = """
        [default]
        auth_type = client_credentials
        login_url = https://old.login.url
        client_id = old_client_id
        client_secret = old_secret
        """

        creds = Credentials(
            login_url="https://new.login.url",
            client_id="new_client_id",
            auth_type=AuthType.CLIENT_CREDENTIALS,
            client_secret="new_secret",
        )

        mock_file = mock_open(read_data=ini_content)

        with (
            patch("os.path.expanduser", return_value="/fake/expanded/path"),
            patch("os.path.exists", return_value=True),
            patch("os.makedirs"),
            patch("builtins.open", mock_file),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds.update_ini(profile="default", ini_file="~/fake_path")

                mock_file.assert_called_with("/fake/expanded/path", "w")
                assert mock_config["default"]["auth_type"] == "client_credentials"
                assert mock_config["default"]["client_id"] == "new_client_id"
                assert mock_config["default"]["client_secret"] == "new_secret"
                assert mock_config["default"]["login_url"] == "https://new.login.url"

    def test_update_ini_new_profile(self):
        """Test updating credentials with a new profile."""
        ini_content = """
        [existing]
        auth_type = oauth_tokens
        login_url = https://existing.login.url
        client_id = existing_client_id
        client_secret = existing_secret
        refresh_token = existing_refresh_token
        """

        creds = Credentials(
            login_url="https://new.profile.login.url",
            client_id="new_profile_client_id",
            auth_type=AuthType.OAUTH_TOKENS,
            client_secret="new_profile_secret",
            refresh_token="new_profile_refresh_token",
        )

        mock_file = mock_open(read_data=ini_content)

        with (
            patch("os.path.expanduser", return_value="/fake/expanded/path"),
            patch("os.path.exists", return_value=True),
            patch("os.makedirs"),
            patch("builtins.open", mock_file),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds.update_ini(profile="new_profile", ini_file="~/fake_path")

                assert "new_profile" in mock_config
                assert (
                    mock_config["new_profile"]["client_id"] == "new_profile_client_id"
                )
                assert (
                    mock_config["new_profile"]["client_secret"] == "new_profile_secret"
                )
                assert (
                    mock_config["new_profile"]["refresh_token"]
                    == "new_profile_refresh_token"
                )
                assert (
                    mock_config["new_profile"]["login_url"]
                    == "https://new.profile.login.url"
                )
                assert (
                    mock_config["existing"]["refresh_token"] == "existing_refresh_token"
                )

    def test_from_available_with_custom_profile(self):
        """Test that from_available uses custom profile when specified."""
        ini_content = """
        [default]
        auth_type = oauth_tokens
        login_url = https://default.login.url
        client_id = default_client_id
        client_secret = default_secret
        refresh_token = default_refresh_token

        [custom_profile]
        auth_type = oauth_tokens
        login_url = https://custom.login.url
        client_id = custom_client_id
        client_secret = custom_secret
        refresh_token = custom_refresh_token
        """

        with (
            patch("datacustomcode.credentials.INI_FILE", "fake_path"),
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds_default = Credentials.from_available()
                assert creds_default.client_secret == "default_secret"
                assert creds_default.refresh_token == "default_refresh_token"
                assert creds_default.login_url == "https://default.login.url"

                creds_custom = Credentials.from_available(profile="custom_profile")
                assert creds_custom.client_id == "custom_client_id"
                assert creds_custom.client_secret == "custom_secret"
                assert creds_custom.refresh_token == "custom_refresh_token"
                assert creds_custom.login_url == "https://custom.login.url"

    # ============== AuthType Enum Tests ==============

    def test_auth_type_values(self):
        """Test AuthType enum values."""
        assert AuthType.OAUTH_TOKENS.value == "oauth_tokens"
        assert AuthType.CLIENT_CREDENTIALS.value == "client_credentials"

    def test_invalid_auth_type_from_env(self):
        """Test that invalid auth type from env raises error."""
        env_vars = {
            "SFDC_LOGIN_URL": "https://test.login.url",
            "SFDC_CLIENT_ID": "test_client_id",
            "SFDC_AUTH_TYPE": "invalid_auth_type",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="Invalid SFDC_AUTH_TYPE"):
                Credentials.from_env()
