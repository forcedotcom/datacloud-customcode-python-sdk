import json
import os
from unittest.mock import mock_open, patch

from click.testing import CliRunner

from datacustomcode.cli import deploy, init


class TestInit:
    @patch("datacustomcode.template.copy_script_template")
    @patch("datacustomcode.scan.update_config")
    @patch("datacustomcode.scan.dc_config_json_from_file")
    @patch("datacustomcode.scan.write_sdk_config")
    @patch("builtins.open", new_callable=mock_open)
    def test_init_command(
        self, mock_file, mock_write_sdk, mock_scan, mock_update, mock_copy
    ):
        """Test init command."""
        mock_scan.return_value = {
            "sdkVersion": "1.0.0",
            "entryPoint": "entrypoint.py",
            "dataspace": "default",
            "permissions": {
                "read": {"dlo": ["input_dlo"]},
                "write": {"dlo": ["output_dlo"]},
            },
        }
        mock_update.return_value = {
            "sdkVersion": "1.0.0",
            "entryPoint": "entrypoint.py",
            "dataspace": "default",
            "permissions": {
                "read": {"dlo": ["input_dlo"]},
                "write": {"dlo": ["output_dlo"]},
            },
        }

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test directory structure
            os.makedirs(os.path.join("test_dir", "payload"), exist_ok=True)

            result = runner.invoke(init, ["test_dir", "--code-type", "script"])

            assert result.exit_code == 0
            mock_copy.assert_called_once_with("test_dir")
            # Verify SDK config was written
            mock_write_sdk.assert_called_once_with("test_dir", {"type": "script"})
            mock_scan.assert_called_once_with(
                os.path.join("test_dir", "payload", "entrypoint.py"), "script"
            )
            mock_update.assert_called_once_with(
                os.path.join("test_dir", "payload", "entrypoint.py")
            )

            # Verify the config.json was written with the correct content
            mock_file.assert_any_call(
                os.path.join("test_dir", "payload", "config.json"), "w"
            )

            # Get all write calls and join them to get the complete written content
            written_content = "".join(
                call.args[0] for call in mock_file().write.call_args_list
            )
            # The last write should be the updated config
            expected_content = json.dumps(mock_update.return_value, indent=2)
            assert expected_content in written_content


class TestDeploy:
    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.credentials.Credentials.from_available")
    def test_deploy_command_success(self, mock_credentials, mock_deploy_full):
        """Test successful deploy command."""
        # Mock credentials
        mock_creds = mock_credentials.return_value

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test payload directory
            os.makedirs("payload", exist_ok=True)

            result = runner.invoke(deploy, ["--name", "test-job", "--version", "1.0.0"])

            assert result.exit_code == 0
            mock_credentials.assert_called_once()
            mock_deploy_full.assert_called_once()

            # Check that deploy_full was called with correct arguments
            call_args = mock_deploy_full.call_args
            assert call_args[0][0] == "payload"  # path
            assert call_args[0][1].name == "test-job"  # metadata
            assert call_args[0][1].version == "1.0.0"
            assert call_args[0][1].description == "Custom Data Transform Code"
            assert call_args[0][2] == mock_creds  # credentials

    @patch("datacustomcode.credentials.Credentials.from_available")
    def test_deploy_command_credentials_error(self, mock_credentials):
        """Test deploy command when credentials are not available."""
        # Mock credentials to raise ValueError
        mock_credentials.side_effect = ValueError(
            "Credentials not found in env or ini file. "
            "Run `datacustomcode configure` to create a credentials file."
        )

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test payload directory
            os.makedirs("payload", exist_ok=True)

            result = runner.invoke(deploy, ["--name", "test-job"])

            assert result.exit_code == 1
            assert "Error: Credentials not found in env or ini file" in result.output

    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.credentials.Credentials.from_available")
    def test_deploy_command_custom_path(self, mock_credentials, mock_deploy_full):
        """Test deploy command with custom path."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test directory
            os.makedirs("custom_path", exist_ok=True)

            result = runner.invoke(
                deploy, ["--path", "custom_path", "--name", "test-job"]
            )

            assert result.exit_code == 0
            mock_deploy_full.assert_called_once()

            # Check that deploy_full was called with custom path
            call_args = mock_deploy_full.call_args
            assert call_args[0][0] == "custom_path"  # path

    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.credentials.Credentials.from_available")
    def test_deploy_command_custom_description(
        self, mock_credentials, mock_deploy_full
    ):
        """Test deploy command with custom description."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test payload directory
            os.makedirs("payload", exist_ok=True)

            result = runner.invoke(
                deploy, ["--name", "test-job", "--description", "Custom description"]
            )

            assert result.exit_code == 0
            mock_deploy_full.assert_called_once()

            # Check that deploy_full was called with custom description
            call_args = mock_deploy_full.call_args
            assert call_args[0][1].description == "Custom description"
