from __future__ import annotations

import os
import subprocess

from datacustomcode.template import script_template_dir

JUPYTERLAB_SH = os.path.join(script_template_dir, "jupyterlab.sh")

# These tests don't actually run the jupyter script. They simply verify
# certain specific configurations of the script for things like syntax
# and security correctness.
#
# These were added when fixing a bug that could have allowed for RCE
# over the local network on the user's device due to previous insufficient
# network config. While not perfect, they do offer a bit of assurance that
# the script is configured correctly.


class TestJupyterlabScript:
    def _read(self) -> str:
        with open(JUPYTERLAB_SH) as f:
            return f.read()

    def test_jupyterlab_sh_syntax_is_valid(self):
        """`bash -n` should accept the script without syntax errors."""
        result = subprocess.run(
            ["bash", "-n", JUPYTERLAB_SH],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr

    def test_start_jupyter_binds_loopback_host_port(self):
        content = self._read()
        assert "-p 127.0.0.1:8888:8888" in content
        assert "-p 8888:8888" not in content

    def test_start_jupyter_binds_container_to_all_interfaces(self):
        content = self._read()
        assert "--ip=0.0.0.0" in content
        assert "--ip=127.0.0.1" not in content

    def test_start_jupyter_generates_token_not_empty_auth(self):
        content = self._read()
        assert "--NotebookApp.token=''" not in content
        assert "--NotebookApp.password=''" not in content
        assert "openssl rand -hex 32" in content

    def test_start_jupyter_uses_dynamic_token_variable(self):
        content = self._read()
        assert "local TOKEN" in content
        assert "TOKEN=$(openssl rand -hex 32)" in content
        assert '--NotebookApp.token="$TOKEN"' in content

    def test_open_browser_url_includes_token_param(self):
        content = self._read()
        assert 'URL="http://localhost:8888/?token=$TOKEN"' in content
        assert "open_browser $URL" in content

    def test_token_never_written_to_file(self):
        content = self._read()
        assert "credentials.ini" not in content
        for line in content.splitlines():
            if "TOKEN" in line:
                assert ">" not in line, f"Line writes TOKEN to a file: {line!r}"
