"""
Integration tests for credentials profile functionality.

This module tests the complete flow of using different credentials profiles
with the DataCloud Custom Code Python SDK components.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from datacustomcode.config import config
from datacustomcode.io.reader.query_api import QueryAPIDataCloudReader
from datacustomcode.io.writer.print import PrintDataCloudWriter


class TestCredentialsProfileIntegration:
    """Test integration of credentials profile functionality across components."""

    def test_query_api_reader_with_custom_profile(self):
        """Test QueryAPIDataCloudReader uses custom credentials profile."""
        mock_spark = MagicMock()

        with patch(
            "datacustomcode.credentials.Credentials.from_available"
        ) as mock_from_available:
            # Mock credentials for custom profile
            mock_credentials = MagicMock()
            mock_credentials.login_url = "https://custom.salesforce.com"
            mock_credentials.username = "custom@example.com"
            mock_credentials.password = "custom_password"
            mock_credentials.client_id = "custom_client_id"
            mock_credentials.client_secret = "custom_secret"
            mock_credentials.dataspace = "custom_dataspace"  # Added dataspace
            mock_from_available.return_value = mock_credentials

            # Mock the SalesforceCDPConnection
            with patch(
                "datacustomcode.io.reader.query_api.SalesforceCDPConnection"
            ) as mock_conn_class:
                mock_conn = MagicMock()
                mock_conn_class.return_value = mock_conn

                # Test with custom profile
                QueryAPIDataCloudReader(
                    mock_spark, credentials_profile="custom_profile"
                )

                # Verify the correct profile was used
                mock_from_available.assert_called_with(profile="custom_profile")

                # Verify the connection was created with the custom credentials including dataspace
                mock_conn_class.assert_called_once_with(
                    "https://custom.salesforce.com",
                    "custom@example.com",
                    "custom_password",
                    "custom_client_id",
                    "custom_secret",
                    dataspace="custom_dataspace",  # Added dataspace parameter
                )

    def test_query_api_reader_without_dataspace(self):
        """Test QueryAPIDataCloudReader works when dataspace is None."""
        mock_spark = MagicMock()

        with patch(
            "datacustomcode.credentials.Credentials.from_available"
        ) as mock_from_available:
            # Mock credentials without dataspace
            mock_credentials = MagicMock()
            mock_credentials.login_url = "https://custom.salesforce.com"
            mock_credentials.username = "custom@example.com"
            mock_credentials.password = "custom_password"
            mock_credentials.client_id = "custom_client_id"
            mock_credentials.client_secret = "custom_secret"
            mock_credentials.dataspace = None  # No dataspace
            mock_from_available.return_value = mock_credentials

            # Mock the SalesforceCDPConnection
            with patch(
                "datacustomcode.io.reader.query_api.SalesforceCDPConnection"
            ) as mock_conn_class:
                mock_conn = MagicMock()
                mock_conn_class.return_value = mock_conn

                # Test with custom profile
                QueryAPIDataCloudReader(
                    mock_spark, credentials_profile="custom_profile"
                )

                # Verify the correct profile was used
                mock_from_available.assert_called_with(profile="custom_profile")

                # Verify the connection was created WITHOUT dataspace parameter when None
                mock_conn_class.assert_called_once_with(
                    "https://custom.salesforce.com",
                    "custom@example.com",
                    "custom_password",
                    "custom_client_id",
                    "custom_secret",
                    # No dataspace parameter when None
                )

    def test_print_writer_with_custom_profile(self):
        """Test PrintDataCloudWriter uses custom credentials profile."""
        mock_spark = MagicMock()

        with patch(
            "datacustomcode.credentials.Credentials.from_available"
        ) as mock_from_available:
            # Mock credentials for custom profile
            mock_credentials = MagicMock()
            mock_credentials.login_url = "https://custom.salesforce.com"
            mock_credentials.username = "custom@example.com"
            mock_credentials.password = "custom_password"
            mock_credentials.client_id = "custom_client_id"
            mock_credentials.client_secret = "custom_secret"
            mock_from_available.return_value = mock_credentials

            # Mock the SalesforceCDPConnection
            with patch(
                "datacustomcode.io.reader.query_api.SalesforceCDPConnection"
            ) as mock_conn_class:
                mock_conn = MagicMock()
                mock_conn_class.return_value = mock_conn

                # Test with custom profile
                writer = PrintDataCloudWriter(
                    mock_spark, credentials_profile="custom_profile"
                )

                # Verify the correct profile was used
                mock_from_available.assert_called_with(profile="custom_profile")

                # Verify the writer has the reader with custom credentials
                assert writer.reader is not None
                assert isinstance(writer.reader, QueryAPIDataCloudReader)

    def test_config_override_with_environment_variable(self):
        """Test that environment variable overrides config credentials profile."""
        # Set environment variable
        os.environ["SFDC_CREDENTIALS_PROFILE"] = "env_profile"

        try:
            # Simulate what happens in entrypoint.py
            credentials_profile = os.environ.get("SFDC_CREDENTIALS_PROFILE", "default")
            assert credentials_profile == "env_profile"

            # Update both reader and writer configs
            if config.reader_config and hasattr(config.reader_config, "options"):
                config.reader_config.options["credentials_profile"] = (
                    credentials_profile
                )

            if config.writer_config and hasattr(config.writer_config, "options"):
                config.writer_config.options["credentials_profile"] = (
                    credentials_profile
                )

            # Verify the configs were updated
            assert config.reader_config.options["credentials_profile"] == "env_profile"
            assert config.writer_config.options["credentials_profile"] == "env_profile"

        finally:
            # Clean up
            del os.environ["SFDC_CREDENTIALS_PROFILE"]

    def test_config_override_programmatically(self):
        """Test programmatic override of credentials profile."""
        custom_profile = "programmatic_profile"

        # Update both reader and writer configs programmatically
        if config.reader_config and hasattr(config.reader_config, "options"):
            config.reader_config.options["credentials_profile"] = custom_profile

        if config.writer_config and hasattr(config.writer_config, "options"):
            config.writer_config.options["credentials_profile"] = custom_profile

        # Verify the configs were updated
        assert config.reader_config.options["credentials_profile"] == custom_profile
        assert config.writer_config.options["credentials_profile"] == custom_profile

    def test_default_profile_behavior(self):
        """Test that default profile is used when no override is specified."""
        # Reset to default values
        if config.reader_config and hasattr(config.reader_config, "options"):
            config.reader_config.options["credentials_profile"] = "default"

        if config.writer_config and hasattr(config.writer_config, "options"):
            config.writer_config.options["credentials_profile"] = "default"

        # Verify default values
        assert config.reader_config.options["credentials_profile"] == "default"
        assert config.writer_config.options["credentials_profile"] == "default"

    def test_credentials_profile_consistency(self):
        """Test that reader and writer use the same credentials profile."""
        mock_spark = MagicMock()
        test_profile = "consistent_profile"

        with patch(
            "datacustomcode.credentials.Credentials.from_available"
        ) as mock_from_available:
            # Mock credentials
            mock_credentials = MagicMock()
            mock_credentials.login_url = "https://consistent.salesforce.com"
            mock_credentials.username = "consistent@example.com"
            mock_credentials.password = "consistent_password"
            mock_credentials.client_id = "consistent_client_id"
            mock_credentials.client_secret = "consistent_secret"
            mock_from_available.return_value = mock_credentials

            # Mock the SalesforceCDPConnection
            with patch(
                "datacustomcode.io.reader.query_api.SalesforceCDPConnection"
            ) as mock_conn_class:
                mock_conn = MagicMock()
                mock_conn_class.return_value = mock_conn

                # Create reader and writer with same profile
                reader = QueryAPIDataCloudReader(
                    mock_spark, credentials_profile=test_profile
                )
                writer = PrintDataCloudWriter(
                    mock_spark, credentials_profile=test_profile
                )

                # Verify both used the same profile
                assert mock_from_available.call_count == 2
                for call in mock_from_available.call_args_list:
                    assert call[1]["profile"] == test_profile

                # Verify both have the same credentials
                assert reader._conn is not None
                assert writer.reader._conn is not None

    def test_multiple_profiles_isolation(self):
        """Test that different profiles are properly isolated."""
        mock_spark = MagicMock()

        with patch(
            "datacustomcode.credentials.Credentials.from_available"
        ) as mock_from_available:
            # Mock different credentials for different profiles
            def mock_credentials_side_effect(profile="default"):
                mock_creds = MagicMock()
                if profile == "profile1":
                    mock_creds.login_url = "https://profile1.salesforce.com"
                    mock_creds.username = "profile1@example.com"
                elif profile == "profile2":
                    mock_creds.login_url = "https://profile2.salesforce.com"
                    mock_creds.username = "profile2@example.com"
                else:  # default
                    mock_creds.login_url = "https://default.salesforce.com"
                    mock_creds.username = "default@example.com"

                mock_creds.password = f"{profile}_password"
                mock_creds.client_id = f"{profile}_client_id"
                mock_creds.client_secret = f"{profile}_secret"
                return mock_creds

            mock_from_available.side_effect = mock_credentials_side_effect

            # Mock the SalesforceCDPConnection
            with patch(
                "datacustomcode.io.reader.query_api.SalesforceCDPConnection"
            ) as mock_conn_class:
                mock_conn = MagicMock()
                mock_conn_class.return_value = mock_conn

                # Create readers with different profiles
                QueryAPIDataCloudReader(mock_spark, credentials_profile="profile1")
                QueryAPIDataCloudReader(mock_spark, credentials_profile="profile2")
                QueryAPIDataCloudReader(mock_spark, credentials_profile="default")

                # Verify each reader used the correct profile
                calls = mock_from_available.call_args_list
                assert len(calls) == 3

                # Check that each call used the correct profile
                profiles_used = [call[1]["profile"] for call in calls]
                assert "profile1" in profiles_used
                assert "profile2" in profiles_used
                assert "default" in profiles_used
