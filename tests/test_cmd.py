from __future__ import annotations

import subprocess
from unittest.mock import Mock, patch

import pytest

from datacustomcode.cmd import (
    CalledProcessError,
    _cmd_output,
    _force_bytes,
    _oserror_to_output,
    cmd_output,
)


class TestCmd:
    def test_force_bytes(self):
        """Test _force_bytes conversion of different types to bytes."""
        # String
        assert _force_bytes("test") == b"test"

        # Already bytes
        assert _force_bytes(b"test") == b"test"

        # Exception with string representation
        error = ValueError("error message")
        assert _force_bytes(error) == b"error message"

        # Object without string representation
        class UnprintableObject:
            def __str__(self):
                raise TypeError()

        obj = UnprintableObject()
        assert b"unprintable UnprintableObject object" in _force_bytes(obj)

    def test_oserror_to_output(self):
        """Test converting OSError to command output format."""
        error = OSError("test error")
        returncode, stdout, stderr = _oserror_to_output(error)

        assert returncode == 1
        assert stdout == b"test error\n"
        assert stderr is None

    def test_called_process_error_formatting(self):
        """Test CalledProcessError string formatting."""
        error = CalledProcessError(
            returncode=1,
            cmd=("ls", "-l"),
            stdout=b"stdout content",
            stderr=b"stderr content",
        )

        error_str = str(error)

        assert "command: ('ls', '-l')" in error_str
        assert "return code: 1" in error_str
        assert "stdout:stdout content" in error_str
        assert "stderr:\n    stderr content" in error_str

        # Test with None stderr
        error = CalledProcessError(
            returncode=1,
            cmd=("ls", "-l"),
            stdout=b"stdout content",
            stderr=None,
        )

        error_str = str(error)
        assert "stderr: (none)" in error_str

    @patch("subprocess.Popen")
    def test_cmd_output_success(self, mock_popen):
        """Test _cmd_output with successful command execution."""
        # Setup mock
        process_mock = Mock()
        process_mock.communicate.return_value = (b"command output", b"")
        process_mock.returncode = 0
        mock_popen.return_value = process_mock

        # Call function
        returncode, stdout, stderr = _cmd_output("ls", "-l")

        # Assertions
        assert returncode == 0
        assert stdout == b"command output"
        assert stderr == b""
        mock_popen.assert_called_once()

        # Check that default kwargs were set
        _, kwargs = mock_popen.call_args
        assert kwargs["stdin"] == subprocess.PIPE
        assert kwargs["stdout"] == subprocess.PIPE
        assert kwargs["stderr"] == subprocess.PIPE

    @patch("subprocess.Popen")
    def test_cmd_output_failure(self, mock_popen):
        """Test _cmd_output with command failure."""
        # Setup mock
        process_mock = Mock()
        process_mock.communicate.return_value = (b"", b"command error")
        process_mock.returncode = 1
        mock_popen.return_value = process_mock

        # Call function with check=True (default)
        with pytest.raises(CalledProcessError) as exc_info:
            _cmd_output("ls", "-l")

        error = exc_info.value
        assert error.returncode == 1
        assert error.stdout == b""
        assert error.stderr == b"command error"

        # Call function with check=False
        returncode, stdout, stderr = _cmd_output("ls", "-l", check=False)
        assert returncode == 1
        assert stdout == b""
        assert stderr == b"command error"

    @patch("subprocess.Popen")
    def test_cmd_output_oserror(self, mock_popen):
        """Test _cmd_output when subprocess.Popen raises OSError."""
        # Setup mock to raise OSError
        mock_popen.side_effect = OSError("command not found")

        # Call function with check=True (default)
        with pytest.raises(CalledProcessError) as exc_info:
            _cmd_output("nonexistent_command")

        error = exc_info.value
        assert error.returncode == 1
        assert b"command not found" in error.stdout

        # Call function with check=False
        returncode, stdout, stderr = _cmd_output("nonexistent_command", check=False)
        assert returncode == 1
        assert b"command not found" in stdout
        assert stderr is None

    @patch("datacustomcode.cmd._cmd_output")
    def test_cmd_output_wrapper(self, mock_cmd_output):
        """Test cmd_output wrapper function."""
        # Setup mock
        mock_cmd_output.return_value = (0, b"command output", b"")

        # Call function
        result = cmd_output("ls", "-l")

        # Assertions
        assert result == "command output"
        mock_cmd_output.assert_called_once_with("ls", "-l")

        # Test with non-zero return code
        mock_cmd_output.return_value = (1, b"", b"error")
        result = cmd_output("ls", "-l", check=False)
        assert result == ""

    @patch("datacustomcode.cmd._cmd_output")
    def test_cmd_output_with_custom_kwargs(self, mock_cmd_output):
        """Test cmd_output with custom keyword arguments."""
        mock_cmd_output.return_value = (0, b"output", None)

        cmd_output("ls", "-l", env={"PATH": "/usr/bin"}, cwd="/tmp")

        # Verify custom kwargs were passed through
        _, kwargs = mock_cmd_output.call_args
        assert kwargs["env"] == {"PATH": "/usr/bin"}
        assert kwargs["cwd"] == "/tmp"
