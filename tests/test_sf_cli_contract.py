"""
Contract tests: verify that the Python CLI accepts exactly the argument signatures
passed by the SF CLI plugin (@salesforce/plugin-data-code-extension v0.1.4) via spawn().

These tests do NOT exercise business logic. They verify that:
  1. All flags recognised by the SF CLI plugin are still accepted by the Python CLI.
  2. Exit code is never 2 (Click's "bad usage" code) for valid SF CLI invocations.
  3. stdout patterns parsed by the SF CLI plugin are present in the Python CLI output.

Source of truth for expected args and stdout regex patterns:
  data-code-extension/src/utils/datacodeBinaryExecutor.ts
"""

import os
import re
from typing import ClassVar
from unittest.mock import patch

from click.testing import CliRunner

from datacustomcode.cli import (
    deploy,
    init,
    run,
    scan,
    zip as zip_cmd,
)
from datacustomcode.deploy import AccessTokenResponse


class TestInitArgContract:
    """
    SF CLI spawn: datacustomcode init --code-type <script|function> <packageDir>
    Ref: executeBinaryInit()
    """

    @patch("datacustomcode.template.copy_script_template")
    @patch("datacustomcode.scan.dc_config_json_from_file")
    @patch("datacustomcode.scan.update_config")
    @patch("datacustomcode.scan.write_sdk_config")
    def test_accepts_code_type_script(
        self, mock_write_sdk, mock_update, mock_scan, mock_copy
    ):
        mock_scan.return_value = {}
        mock_update.return_value = {}
        runner = CliRunner()
        with runner.isolated_filesystem():
            os.makedirs(os.path.join("mydir", "payload"), exist_ok=True)
            result = runner.invoke(init, ["--code-type", "script", "mydir"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.template.copy_function_template")
    @patch("datacustomcode.scan.dc_config_json_from_file")
    @patch("datacustomcode.scan.update_config")
    @patch("datacustomcode.scan.write_sdk_config")
    def test_accepts_code_type_function(
        self, mock_write_sdk, mock_update, mock_scan, mock_copy
    ):
        mock_scan.return_value = {}
        mock_update.return_value = {}
        runner = CliRunner()
        with runner.isolated_filesystem():
            os.makedirs(os.path.join("mydir", "payload"), exist_ok=True)
            result = runner.invoke(init, ["--code-type", "function", "mydir"])
        assert result.exit_code != 2, result.output


class TestScanArgContract:
    """
    SF CLI spawn: datacustomcode scan [--dry-run] [--no-requirements]
    [--config <file>] <entrypoint>
    Ref: executeBinaryScan()
    """

    @patch("datacustomcode.scan.update_config")
    def test_accepts_positional_entrypoint(self, mock_update):
        mock_update.return_value = {}
        runner = CliRunner()
        result = runner.invoke(scan, ["--dry-run", "payload/entrypoint.py"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.scan.update_config")
    def test_accepts_dry_run_flag(self, mock_update):
        mock_update.return_value = {}
        runner = CliRunner()
        result = runner.invoke(scan, ["--dry-run", "payload/entrypoint.py"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.scan.update_config")
    def test_accepts_no_requirements_flag(self, mock_update):
        mock_update.return_value = {}
        runner = CliRunner()
        result = runner.invoke(
            scan, ["--no-requirements", "--dry-run", "payload/entrypoint.py"]
        )
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.scan.update_config")
    def test_accepts_config_flag(self, mock_update):
        mock_update.return_value = {}
        runner = CliRunner()
        result = runner.invoke(
            scan,
            ["--config", "custom/config.json", "--dry-run", "payload/entrypoint.py"],
        )
        assert result.exit_code != 2, result.output


class TestZipArgContract:
    """
    SF CLI spawn: datacustomcode zip [--network <network>] <packageDir>
    Ref: executeBinaryZip()
    """

    @patch("datacustomcode.deploy.zip")
    @patch("datacustomcode.scan.find_base_directory")
    @patch("datacustomcode.scan.get_package_type")
    def test_accepts_positional_packagedir(
        self, mock_pkg_type, mock_find_base, mock_zip
    ):
        mock_find_base.return_value = "payload"
        mock_pkg_type.return_value = "script"
        runner = CliRunner()
        result = runner.invoke(zip_cmd, ["payload"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.deploy.zip")
    @patch("datacustomcode.scan.find_base_directory")
    @patch("datacustomcode.scan.get_package_type")
    def test_accepts_network_flag(self, mock_pkg_type, mock_find_base, mock_zip):
        mock_find_base.return_value = "payload"
        mock_pkg_type.return_value = "script"
        runner = CliRunner()
        result = runner.invoke(zip_cmd, ["--network", "custom", "payload"])
        assert result.exit_code != 2, result.output


class TestDeployArgContract:
    """
    SF CLI spawn:
        datacustomcode deploy
            --name <name> --version <ver> --description <desc>
            --path <dir> --sf-cli-org <org> --cpu-size <size>
            [--network <net>] [--function-invoke-opt <opt>]
    Ref: executeBinaryDeploy()
    """

    _BASE_ARGS: ClassVar[list[str]] = [
        "--name", "my-pkg",
        "--version", "1.0.0",
        "--description", "My description",
        "--path", "payload",
        "--sf-cli-org", "my-org",
        "--cpu-size", "CPU_2XL",
    ]  # fmt: skip

    @patch("datacustomcode.deploy._retrieve_access_token_from_sf_cli")
    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.cli.find_base_directory")
    @patch("datacustomcode.cli.get_package_type")
    def test_accepts_required_flags(
        self, mock_pkg_type, mock_find_base, mock_deploy_full, mock_sf_cli_token
    ):
        mock_find_base.return_value = "payload"
        mock_pkg_type.return_value = "script"
        mock_sf_cli_token.return_value = AccessTokenResponse(
            access_token="tok", instance_url="https://example.com"
        )
        runner = CliRunner()
        result = runner.invoke(deploy, self._BASE_ARGS)
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.deploy._retrieve_access_token_from_sf_cli")
    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.cli.find_base_directory")
    @patch("datacustomcode.cli.get_package_type")
    def test_accepts_network_flag(
        self, mock_pkg_type, mock_find_base, mock_deploy_full, mock_sf_cli_token
    ):
        mock_find_base.return_value = "payload"
        mock_pkg_type.return_value = "script"
        mock_sf_cli_token.return_value = AccessTokenResponse(
            access_token="tok", instance_url="https://example.com"
        )
        runner = CliRunner()
        result = runner.invoke(deploy, [*self._BASE_ARGS, "--network", "custom"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.deploy._retrieve_access_token_from_sf_cli")
    @patch("datacustomcode.deploy.deploy_full")
    @patch("datacustomcode.cli.find_base_directory")
    @patch("datacustomcode.cli.get_package_type")
    def test_accepts_function_invoke_opt_flag(
        self, mock_pkg_type, mock_find_base, mock_deploy_full, mock_sf_cli_token
    ):
        mock_find_base.return_value = "payload"
        mock_pkg_type.return_value = "function"
        mock_sf_cli_token.return_value = AccessTokenResponse(
            access_token="tok", instance_url="https://example.com"
        )
        runner = CliRunner()
        result = runner.invoke(
            deploy, [*self._BASE_ARGS, "--function-invoke-opt", "ASYNC"]
        )
        assert result.exit_code != 2, result.output


class TestRunArgContract:
    """
    SF CLI spawn:
        datacustomcode run --sf-cli-org <org>
            [--config-file <file>] [--dependencies <deps>] <packageDir>
    Ref: executeBinaryRun()

    Known incompatibility: SF CLI passes `--dependencies` once as a single string.
    Python CLI declares multiple=True, so the value arrives as a 1-tuple containing
    the raw string rather than individual dep names.
    """

    @patch("datacustomcode.run.run_entrypoint")
    def test_accepts_sf_cli_org_and_positional(self, mock_run):
        runner = CliRunner()
        result = runner.invoke(run, ["--sf-cli-org", "my-org", "payload/entrypoint.py"])
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.run.run_entrypoint")
    def test_accepts_config_file_flag(self, mock_run):
        runner = CliRunner()
        result = runner.invoke(
            run,
            [
                "--sf-cli-org",
                "my-org",
                "--config-file",
                "payload/config.json",
                "payload/entrypoint.py",
            ],
        )
        assert result.exit_code != 2, result.output

    @patch("datacustomcode.run.run_entrypoint")
    def test_accepts_dependencies_as_single_string(self, mock_run):
        """SF CLI passes --dependencies once as a comma-separated string.

        Python CLI uses multiple=True, so run_entrypoint receives ("dep1,dep2",)
        not ("dep1", "dep2"). The string is NOT split on commas.
        """
        runner = CliRunner()
        result = runner.invoke(
            run,
            [
                "--sf-cli-org",
                "my-org",
                "--dependencies",
                "dep1,dep2",
                "payload/entrypoint.py",
            ],
        )
        assert result.exit_code != 2, result.output
        # Document the incompatibility: SF CLI passes a single "dep1,dep2" string,
        # but run_entrypoint receives ("dep1,dep2",) — not ("dep1", "dep2").
        assert mock_run.call_args[0][2] == ("dep1,dep2",)


class TestSfCliOutputRegexContract:
    """
    The SF CLI plugin parses stdout from each command with regex patterns.
    These tests verify that the Python CLI's actual output matches what the
    plugin expects (v0.1.4).

    Ref: stdout parsing in each executeBinary*() method of datacodeBinaryExecutor.ts.
    """

    # ── init ──────────────────────────────────────────────────────────────────

    @patch("datacustomcode.template.copy_script_template")
    @patch("datacustomcode.scan.dc_config_json_from_file")
    @patch("datacustomcode.scan.update_config")
    @patch("datacustomcode.scan.write_sdk_config")
    def test_init_copying_template_pattern(
        self, mock_write_sdk, mock_update, mock_scan, mock_copy
    ):
        """SF CLI parses 'Copying template to <dir>' from init stdout."""
        mock_scan.return_value = {}
        mock_update.return_value = {}
        runner = CliRunner()
        with runner.isolated_filesystem():
            os.makedirs(os.path.join("mydir", "payload"), exist_ok=True)
            result = runner.invoke(init, ["--code-type", "script", "mydir"])
        assert re.search(r"Copying template to .+", result.output)

    # ── scan ──────────────────────────────────────────────────────────────────

    @patch("datacustomcode.scan.update_config")
    def test_scan_scanning_file_pattern(self, mock_update):
        """SF CLI parses 'Scanning <file>...' from scan stdout."""
        mock_update.return_value = {}
        runner = CliRunner()
        result = runner.invoke(scan, ["--dry-run", "payload/entrypoint.py"])
        assert re.search(r"Scanning .+\.\.\.", result.output)
