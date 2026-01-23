# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import queue
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

import click
import pytest
import requests

from datacustomcode.auth import (
    OAuthCallbackHandler,
    _exchange_code_for_tokens,
    _run_oauth_callback_server,
    configure_oauth_tokens,
    do_oauth_browser_flow,
)


class TestOAuthCallbackHandler:
    """Test suite for OAuthCallbackHandler."""

    @patch("datacustomcode.auth.http.server.SimpleHTTPRequestHandler.__init__")
    def test_do_get_with_code(self, mock_init):
        """Test handling GET request with authorization code."""
        auth_code_queue = queue.Queue()
        mock_init.return_value = None

        handler = OAuthCallbackHandler(
            request=Mock(),
            client_address=("127.0.0.1", 5555),
            server=Mock(),
            auth_code_queue=auth_code_queue,
        )

        # Mock the request path with code
        handler.path = "/callback?code=test_auth_code_123"
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = Mock()

        handler.do_GET()

        # Verify code was put in queue
        assert auth_code_queue.get() == "test_auth_code_123"
        handler.send_response.assert_called_once_with(200)
        handler.send_header.assert_called_with("Content-type", "text/html")
        handler.end_headers.assert_called_once()
        handler.wfile.write.assert_called_once()

    @patch("datacustomcode.auth.http.server.SimpleHTTPRequestHandler.__init__")
    def test_do_get_with_error(self, mock_init):
        """Test handling GET request with OAuth error."""
        auth_code_queue = queue.Queue()
        mock_init.return_value = None

        handler = OAuthCallbackHandler(
            request=Mock(),
            client_address=("127.0.0.1", 5555),
            server=Mock(),
            auth_code_queue=auth_code_queue,
        )

        handler.path = "/callback?error=access_denied&error_description=User%20denied"
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = Mock()

        handler.do_GET()

        # Verify error was put in queue
        result = auth_code_queue.get()
        assert result.startswith("ERROR:")
        assert "access_denied" in result
        handler.send_response.assert_called_once_with(400)

    @patch("datacustomcode.auth.http.server.SimpleHTTPRequestHandler.__init__")
    def test_do_get_invalid_callback(self, mock_init):
        """Test handling GET request without code or error."""
        auth_code_queue = queue.Queue()
        mock_init.return_value = None

        handler = OAuthCallbackHandler(
            request=Mock(),
            client_address=("127.0.0.1", 5555),
            server=Mock(),
            auth_code_queue=auth_code_queue,
        )

        handler.path = "/callback"
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = Mock()

        handler.do_GET()

        # Verify no code was put in queue
        assert auth_code_queue.empty()
        handler.send_response.assert_called_once_with(400)

    @patch("datacustomcode.auth.http.server.SimpleHTTPRequestHandler.__init__")
    def test_log_message_suppressed(self, mock_init):
        """Test that log messages are suppressed."""
        mock_init.return_value = None
        handler = OAuthCallbackHandler(
            request=Mock(),
            client_address=("127.0.0.1", 5555),
            server=Mock(),
        )
        # Should not raise any exceptions
        handler.log_message("test", "arg1", "arg2")


class TestRunOAuthCallbackServer:
    """Test suite for _run_oauth_callback_server."""

    @patch("datacustomcode.auth.socketserver.TCPServer")
    @patch("datacustomcode.auth.threading.Thread")
    @patch("datacustomcode.auth.time.sleep")
    def test_run_oauth_callback_server_valid_uri(
        self, mock_sleep, mock_thread, mock_tcpserver
    ):
        """Test starting callback server with valid redirect URI."""
        redirect_uri = "http://localhost:5555/callback"
        auth_code_queue = queue.Queue()

        mock_server = MagicMock()
        mock_server.server_address = ("localhost", 5555)
        mock_tcpserver.return_value = mock_server

        server, port = _run_oauth_callback_server(redirect_uri, auth_code_queue)

        assert port == 5555
        mock_tcpserver.assert_called_once()
        mock_thread.assert_called_once()
        mock_sleep.assert_called_once_with(0.5)
        assert mock_server.allow_reuse_address is True

    def test_run_oauth_callback_server_invalid_uri_no_host(self):
        """Test starting callback server with invalid URI (no host)."""
        redirect_uri = "http:///callback"
        auth_code_queue = queue.Queue()

        with pytest.raises(ValueError, match="Invalid redirect URI"):
            _run_oauth_callback_server(redirect_uri, auth_code_queue)

    def test_run_oauth_callback_server_invalid_uri_no_port(self):
        """Test starting callback server with invalid URI (no port)."""
        redirect_uri = "http://localhost/callback"
        auth_code_queue = queue.Queue()

        with pytest.raises(ValueError, match="Invalid redirect URI"):
            _run_oauth_callback_server(redirect_uri, auth_code_queue)

    @patch("datacustomcode.auth.socketserver.TCPServer")
    @patch("datacustomcode.auth.threading.Thread")
    @patch("datacustomcode.auth.time.sleep")
    def test_run_oauth_callback_server_different_port(
        self, mock_sleep, mock_thread, mock_tcpserver
    ):
        """Test starting callback server on different port."""
        redirect_uri = "http://localhost:8080/callback"
        auth_code_queue = queue.Queue()

        mock_server = MagicMock()
        mock_server.server_address = ("localhost", 8080)
        mock_tcpserver.return_value = mock_server

        server, port = _run_oauth_callback_server(redirect_uri, auth_code_queue)

        assert port == 8080
        mock_tcpserver.assert_called_once()
        mock_thread.assert_called_once()
        # Verify server was started in a thread
        assert mock_server.allow_reuse_address is True


class TestExchangeCodeForTokens:
    """Test suite for _exchange_code_for_tokens."""

    @patch("datacustomcode.auth.requests.post")
    def test_exchange_code_for_tokens_success(self, mock_post):
        """Test successful token exchange."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "instance_url": "https://instance.example.com",
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        result = _exchange_code_for_tokens(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_client_secret",
            redirect_uri="http://localhost:5555/callback",
            auth_code="test_auth_code",
        )

        assert result["access_token"] == "test_access_token"
        assert result["refresh_token"] == "test_refresh_token"
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "services/oauth2/token" in call_args[0][0]
        assert call_args[1]["data"]["grant_type"] == "authorization_code"
        assert call_args[1]["data"]["code"] == "test_auth_code"
        assert call_args[1]["data"]["client_id"] == "test_client_id"
        assert call_args[1]["data"]["client_secret"] == "test_client_secret"
        assert call_args[1]["data"]["redirect_uri"] == "http://localhost:5555/callback"

    @patch("datacustomcode.auth.requests.post")
    def test_exchange_code_for_tokens_http_error(self, mock_post):
        """Test token exchange with HTTP error."""
        mock_post.side_effect = requests.exceptions.HTTPError("Server error")

        with pytest.raises(click.ClickException, match="Failed to exchange"):
            _exchange_code_for_tokens(
                login_url="https://login.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                redirect_uri="http://localhost:5555/callback",
                auth_code="test_auth_code",
            )

    @patch("datacustomcode.auth.requests.post")
    def test_exchange_code_for_tokens_connection_error(self, mock_post):
        """Test token exchange with connection error."""
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with pytest.raises(click.ClickException, match="Failed to exchange"):
            _exchange_code_for_tokens(
                login_url="https://login.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                redirect_uri="http://localhost:5555/callback",
                auth_code="test_auth_code",
            )

    @patch("datacustomcode.auth.requests.post")
    def test_exchange_code_for_tokens_timeout(self, mock_post):
        """Test token exchange with timeout."""
        mock_post.side_effect = requests.exceptions.Timeout("Request timeout")

        with pytest.raises(click.ClickException, match="Failed to exchange"):
            _exchange_code_for_tokens(
                login_url="https://login.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                redirect_uri="http://localhost:5555/callback",
                auth_code="test_auth_code",
            )


class TestDoOAuthBrowserFlow:
    """Test suite for do_oauth_browser_flow."""

    @patch("datacustomcode.auth.webbrowser.open")
    @patch("datacustomcode.auth._exchange_code_for_tokens")
    @patch("datacustomcode.auth._run_oauth_callback_server")
    @patch("datacustomcode.auth.click.echo")
    def test_do_oauth_browser_flow_success(
        self, mock_echo, mock_server, mock_exchange, mock_browser
    ):
        """Test successful OAuth browser flow."""
        # Setup mocks
        mock_server_instance = MagicMock()
        auth_code_queue = queue.Queue()
        auth_code_queue.put("test_auth_code")

        def server_factory(redirect_uri, queue):
            return (mock_server_instance, 5555)

        mock_server.side_effect = server_factory
        mock_exchange.return_value = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
        }

        # Mock queue.Queue to return our queue with the code already in it
        with patch("datacustomcode.auth.queue.Queue") as mock_queue_class:
            mock_queue_class.return_value = auth_code_queue

            result = do_oauth_browser_flow(
                login_url="https://login.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                redirect_uri="http://localhost:5555/callback",
            )

            assert result == ("test_refresh_token", "test_access_token")
            mock_exchange.assert_called_once_with(
                "https://login.salesforce.com",
                "test_client_id",
                "test_client_secret",
                "http://localhost:5555/callback",
                "test_auth_code",
            )
            mock_server_instance.shutdown.assert_called()

    @patch("datacustomcode.auth.webbrowser.open")
    @patch("datacustomcode.auth._run_oauth_callback_server")
    @patch("datacustomcode.auth.click.echo")
    def test_do_oauth_browser_flow_timeout(self, mock_echo, mock_server, mock_browser):
        """Test OAuth browser flow with timeout."""
        mock_server_instance = MagicMock()

        def server_factory(redirect_uri, queue):
            return (mock_server_instance, 5555)

        mock_server.side_effect = server_factory

        # Create a mock queue that raises Empty immediately
        mock_queue = MagicMock()
        mock_queue.get.side_effect = queue.Empty()

        with patch("datacustomcode.auth.queue.Queue") as mock_queue_class:
            mock_queue_class.return_value = mock_queue

            with pytest.raises(click.ClickException, match="Authentication timeout"):
                do_oauth_browser_flow(
                    login_url="https://login.salesforce.com",
                    client_id="test_client_id",
                    client_secret="test_client_secret",
                    redirect_uri="http://localhost:5555/callback",
                )

            mock_server_instance.shutdown.assert_called()

    @patch("datacustomcode.auth.webbrowser.open")
    @patch("datacustomcode.auth._run_oauth_callback_server")
    @patch("datacustomcode.auth.click.echo")
    def test_do_oauth_browser_flow_oauth_error(
        self, mock_echo, mock_server, mock_browser
    ):
        """Test OAuth browser flow with OAuth error response."""
        mock_server_instance = MagicMock()

        def server_factory(redirect_uri, queue):
            return (mock_server_instance, 5555)

        mock_server.side_effect = server_factory

        # Create a queue with error message
        error_queue = queue.Queue()
        error_queue.put("ERROR:access_denied:User denied")

        with patch("datacustomcode.auth.queue.Queue") as mock_queue_class:
            mock_queue_class.return_value = error_queue

            with pytest.raises(click.ClickException, match="OAuth error"):
                do_oauth_browser_flow(
                    login_url="https://login.salesforce.com",
                    client_id="test_client_id",
                    client_secret="test_client_secret",
                    redirect_uri="http://localhost:5555/callback",
                )

            mock_server_instance.shutdown.assert_called()

    @patch("datacustomcode.auth.webbrowser.open")
    @patch("datacustomcode.auth._exchange_code_for_tokens")
    @patch("datacustomcode.auth._run_oauth_callback_server")
    @patch("datacustomcode.auth.click.echo")
    def test_do_oauth_browser_flow_no_refresh_token(
        self, mock_echo, mock_server, mock_exchange, mock_browser
    ):
        """Test OAuth browser flow when no refresh token is returned."""
        mock_server_instance = MagicMock()

        def server_factory(redirect_uri, queue):
            return (mock_server_instance, 5555)

        mock_server.side_effect = server_factory
        mock_exchange.return_value = {
            "access_token": "test_access_token",
            # No refresh_token
        }

        auth_code_queue = queue.Queue()
        auth_code_queue.put("test_auth_code")

        with patch("datacustomcode.auth.queue.Queue") as mock_queue_class:
            mock_queue_class.return_value = auth_code_queue

            with pytest.raises(click.ClickException, match="No refresh_token"):
                do_oauth_browser_flow(
                    login_url="https://login.salesforce.com",
                    client_id="test_client_id",
                    client_secret="test_client_secret",
                    redirect_uri="http://localhost:5555/callback",
                )


class TestConfigureOAuthTokens:
    """Test suite for configure_oauth_tokens."""

    @patch("datacustomcode.credentials.Credentials")
    @patch("datacustomcode.auth.do_oauth_browser_flow")
    @patch("datacustomcode.auth.click.secho")
    def test_configure_oauth_tokens_success(
        self, mock_secho, mock_browser_flow, mock_credentials_class
    ):
        """Test successful OAuth token configuration."""
        mock_browser_flow.return_value = (
            "test_refresh_token",
            "test_access_token",
        )
        mock_credentials_instance = MagicMock()
        mock_credentials_class.return_value = mock_credentials_instance

        configure_oauth_tokens(
            login_url="https://login.salesforce.com",
            client_id="test_client_id",
            client_secret="test_client_secret",
            redirect_uri="http://localhost:5555/callback",
            profile="default",
        )

        mock_browser_flow.assert_called_once_with(
            "https://login.salesforce.com",
            "test_client_id",
            "test_client_secret",
            "http://localhost:5555/callback",
        )
        # Verify Credentials was called with correct parameters
        call_kwargs = mock_credentials_class.call_args[1]
        assert call_kwargs["login_url"] == "https://login.salesforce.com"
        assert call_kwargs["client_id"] == "test_client_id"
        assert call_kwargs["client_secret"] == "test_client_secret"
        assert call_kwargs["refresh_token"] == "test_refresh_token"
        assert call_kwargs["access_token"] == "test_access_token"
        assert call_kwargs["redirect_uri"] == "http://localhost:5555/callback"
        mock_credentials_instance.update_ini.assert_called_once_with(profile="default")
        # Should be called twice: once for error (if any) and once for success
        assert mock_secho.call_count >= 1

    @patch("datacustomcode.auth.do_oauth_browser_flow")
    @patch("datacustomcode.auth.click.secho")
    def test_configure_oauth_tokens_browser_flow_error(
        self, mock_secho, mock_browser_flow
    ):
        """Test OAuth token configuration with browser flow error."""
        mock_browser_flow.side_effect = click.ClickException("Authentication failed")

        with pytest.raises(click.Abort):
            configure_oauth_tokens(
                login_url="https://login.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                redirect_uri="http://localhost:5555/callback",
                profile="default",
            )

        mock_secho.assert_called_once()
