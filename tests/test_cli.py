import json
import os
from unittest.mock import mock_open, patch

from click.testing import CliRunner

from datacustomcode.cli import init


class TestInit:
    @patch("datacustomcode.template.copy_template")
    @patch("datacustomcode.scan.dc_config_json_from_file")
    @patch("builtins.open", new_callable=mock_open)
    def test_init_command(self, mock_file, mock_scan, mock_copy):
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

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test directory structure
            os.makedirs(os.path.join("test_dir", "payload"), exist_ok=True)

            result = runner.invoke(init, ["test_dir"])

            assert result.exit_code == 0
            mock_copy.assert_called_once_with("test_dir")
            mock_scan.assert_called_once_with(
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
            expected_content = json.dumps(mock_scan.return_value, indent=2)
            assert written_content == expected_content
