from __future__ import annotations

import configparser
import os
from unittest.mock import mock_open, patch

import pytest

from datacustomcode.credentials import ENV_CREDENTIALS, Credentials


class TestCredentials:
    def test_from_env(self):
        """Test loading credentials from environment variables."""
        test_creds = {
            "username": "test_user",
            "password": "test_pass",
            "client_id": "test_client_id",
            "client_secret": "test_secret",
            "login_url": "https://test.login.url",
        }

        with patch.dict(
            os.environ, {v: test_creds[k] for k, v in ENV_CREDENTIALS.items()}
        ):
            creds = Credentials.from_env()

            assert creds.username == test_creds["username"]
            assert creds.password == test_creds["password"]
            assert creds.client_id == test_creds["client_id"]
            assert creds.client_secret == test_creds["client_secret"]
            assert creds.login_url == test_creds["login_url"]

    def test_from_env_missing_vars(self):
        """Test that missing environment variables raise appropriate error."""
        # Ensure environment variables are not set
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="must be set in environment"):
                Credentials.from_env()

    def test_from_ini(self):
        """Test loading credentials from an INI file."""
        ini_content = """
        [default]
        username = ini_user
        password = ini_pass
        client_id = ini_client_id
        client_secret = ini_secret
        login_url = https://ini.login.url

        [other_profile]
        username = other_user
        password = other_pass
        client_id = other_client_id
        client_secret = other_secret
        login_url = https://other.login.url
        """

        with (
            patch("configparser.ConfigParser.read"),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):

            # Mock the configparser behavior for reading the file
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                # Test default profile
                creds = Credentials.from_ini(profile="default", ini_file="fake_path")
                assert creds.username == "ini_user"
                assert creds.password == "ini_pass"
                assert creds.client_id == "ini_client_id"
                assert creds.client_secret == "ini_secret"
                assert creds.login_url == "https://ini.login.url"

                # Test other profile
                creds = Credentials.from_ini(
                    profile="other_profile", ini_file="fake_path"
                )
                assert creds.username == "other_user"
                assert creds.password == "other_pass"
                assert creds.client_id == "other_client_id"
                assert creds.client_secret == "other_secret"
                assert creds.login_url == "https://other.login.url"

    def test_from_available_env(self):
        """Test that from_available uses environment variables when available."""
        test_creds = {
            "username": "test_user",
            "password": "test_pass",
            "client_id": "test_client_id",
            "client_secret": "test_secret",
            "login_url": "https://test.login.url",
        }

        with (
            patch.dict(
                os.environ, {v: test_creds[k] for k, v in ENV_CREDENTIALS.items()}
            ),
            patch("os.path.exists", return_value=False),
        ):
            creds = Credentials.from_available()

            assert creds.username == test_creds["username"]
            assert creds.password == test_creds["password"]
            assert creds.client_id == test_creds["client_id"]
            assert creds.client_secret == test_creds["client_secret"]
            assert creds.login_url == test_creds["login_url"]

    def test_from_available_ini(self):
        """Test that from_available uses INI file when env vars not available."""
        ini_content = """
        [default]
        username = ini_user
        password = ini_pass
        client_id = ini_client_id
        client_secret = ini_secret
        login_url = https://ini.login.url
        """

        with (
            patch.dict(os.environ, {}, clear=True),
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=ini_content)),
        ):

            # Mock the configparser behavior
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds = Credentials.from_available()

                assert creds.username == "ini_user"
                assert creds.password == "ini_pass"
                assert creds.client_id == "ini_client_id"
                assert creds.client_secret == "ini_secret"
                assert creds.login_url == "https://ini.login.url"

    def test_from_available_no_creds(self):
        """Test that from_available raises error when no credentials are found."""
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("os.path.exists", return_value=False),
        ):
            with pytest.raises(ValueError, match="Credentials not found"):
                Credentials.from_available()

    def test_update_ini(self):
        """Test updating credentials in an INI file."""
        ini_content = """
        [default]
        username = old_user
        password = old_pass
        client_id = old_client_id
        client_secret = old_secret
        login_url = https://old.login.url
        """

        creds = Credentials(
            username="new_user",
            password="new_pass",
            client_id="new_client_id",
            client_secret="new_secret",
            login_url="https://new.login.url",
        )

        mock_file = mock_open(read_data=ini_content)

        with (
            patch("os.path.expanduser", return_value="/fake/expanded/path"),
            patch("os.path.exists", return_value=True),
            patch("os.makedirs"),
            patch("builtins.open", mock_file),
        ):

            # Mock the configparser behavior
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds.update_ini(profile="default", ini_file="~/fake_path")

                # Check if the file was opened for writing
                mock_file.assert_called_with("/fake/expanded/path", "w")

                # Check if the config has the updated values
                assert mock_config["default"]["username"] == "new_user"
                assert mock_config["default"]["password"] == "new_pass"
                assert mock_config["default"]["client_id"] == "new_client_id"
                assert mock_config["default"]["client_secret"] == "new_secret"
                assert mock_config["default"]["login_url"] == "https://new.login.url"

    def test_update_ini_new_profile(self):
        """Test updating credentials with a new profile."""
        ini_content = """
        [existing]
        username = existing_user
        password = existing_pass
        client_id = existing_client_id
        client_secret = existing_secret
        login_url = https://existing.login.url
        """

        creds = Credentials(
            username="new_profile_user",
            password="new_profile_pass",
            client_id="new_profile_client_id",
            client_secret="new_profile_secret",
            login_url="https://new.profile.login.url",
        )

        mock_file = mock_open(read_data=ini_content)

        with (
            patch("os.path.expanduser", return_value="/fake/expanded/path"),
            patch("os.path.exists", return_value=True),
            patch("os.makedirs"),
            patch("builtins.open", mock_file),
        ):

            # Mock the configparser behavior
            mock_config = configparser.ConfigParser()
            mock_config.read_string(ini_content)

            with patch.object(configparser, "ConfigParser", return_value=mock_config):
                creds.update_ini(profile="new_profile", ini_file="~/fake_path")

                # Check if the new profile was created
                assert "new_profile" in mock_config
                assert mock_config["new_profile"]["username"] == "new_profile_user"
                assert mock_config["new_profile"]["password"] == "new_profile_pass"
                assert (
                    mock_config["new_profile"]["client_id"] == "new_profile_client_id"
                )
                assert (
                    mock_config["new_profile"]["client_secret"] == "new_profile_secret"
                )
                assert (
                    mock_config["new_profile"]["login_url"]
                    == "https://new.profile.login.url"
                )

                # Check that existing profile was not modified
                assert mock_config["existing"]["username"] == "existing_user"
