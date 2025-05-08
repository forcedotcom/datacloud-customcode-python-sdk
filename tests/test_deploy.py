"""Tests for the deploy module."""

from unittest.mock import (
    MagicMock,
    mock_open,
    patch,
)

import pytest
import requests

from datacustomcode.credentials import Credentials
from datacustomcode.deploy import DloPermission, Permissions

# Patch get_version before importing deploy module
with patch("datacustomcode.version.get_version", return_value="1.2.3"):
    from datacustomcode.deploy import (
        AccessTokenResponse,
        CreateDeploymentResponse,
        DataTransformConfig,
        DeploymentsResponse,
        TransformationJobMetadata,
        _make_api_call,
        _retrieve_access_token,
        create_data_transform,
        create_deployment,
        deploy_full,
        get_data_transform_config,
        get_deployments,
        run_data_transform,
        verify_data_transform_config,
        wait_for_deployment,
        zip_and_upload_directory,
    )


class TestMakeApiCall:
    @patch("datacustomcode.deploy.requests.request")
    def test_make_api_call_with_token(self, mock_request):
        """Test API call with authentication token."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"key": "value"}
        mock_request.return_value = mock_response

        result = _make_api_call(
            "https://example.com", "POST", token="test_token", json={"data": "value"}
        )

        mock_request.assert_called_once_with(
            method="POST",
            url="https://example.com",
            headers={"Authorization": "Bearer test_token"},
            json={"data": "value"},
        )
        assert result == {"key": "value"}

    @patch("datacustomcode.deploy.requests.request")
    def test_make_api_call_invalid_response(self, mock_request):
        """Test API call with non-dict response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = ["list", "response"]  # Non-dict response
        mock_request.return_value = mock_response

        with pytest.raises(AssertionError, match="Unexpected response type"):
            _make_api_call("https://example.com", "GET")


class TestRetrieveAccessToken:
    @patch("datacustomcode.deploy._make_api_call")
    def test_retrieve_access_token(self, mock_make_api_call):
        """Test retrieving access token."""
        credentials = Credentials(
            username="user",
            password="pass",
            client_id="id",
            client_secret="secret",
            login_url="https://example.com",
        )

        mock_make_api_call.return_value = {
            "access_token": "test_token",
            "instance_url": "https://instance.example.com",
        }

        result = _retrieve_access_token(credentials)

        mock_make_api_call.assert_called_once()
        assert isinstance(result, AccessTokenResponse)
        assert result.access_token == "test_token"
        assert result.instance_url == "https://instance.example.com"


class TestCreateDeployment:
    @patch("datacustomcode.deploy._make_api_call")
    def test_create_deployment_success(self, mock_make_api_call):
        """Test successful deployment creation."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        mock_make_api_call.return_value = {
            "fileUploadUrl": "https://upload.example.com"
        }

        result = create_deployment(access_token, metadata)

        mock_make_api_call.assert_called_once()
        assert isinstance(result, CreateDeploymentResponse)
        assert result.fileUploadUrl == "https://upload.example.com"

    @patch("datacustomcode.deploy._make_api_call")
    def test_create_deployment_conflict(self, mock_make_api_call):
        """Test deployment creation with conflict response."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        # Mock HTTP error with 409 Conflict
        mock_response = MagicMock()
        mock_response.status_code = 409
        http_error = requests.HTTPError("Deployment exists")
        http_error.response = mock_response
        mock_make_api_call.side_effect = http_error

        with pytest.raises(ValueError, match="Deployment test_job exists"):
            create_deployment(access_token, metadata)


class TestZipAndUploadDirectory:
    @patch("datacustomcode.deploy.shutil.make_archive")
    @patch("datacustomcode.deploy.requests.put")
    @patch("builtins.open", new_callable=mock_open, read_data=b"test data")
    def test_zip_and_upload_directory(self, mock_file, mock_put, mock_make_archive):
        """Test zipping and uploading a directory."""
        mock_response = MagicMock()
        mock_put.return_value = mock_response

        zip_and_upload_directory("/test/dir", "https://upload.example.com")

        mock_make_archive.assert_called_once_with("deployment", "zip", "/test/dir")
        mock_file.assert_called_once_with("deployment.zip", "rb")
        mock_put.assert_called_once()
        mock_response.raise_for_status.assert_called_once()


class TestGetDeployments:
    @patch("datacustomcode.deploy._make_api_call")
    def test_get_deployments(self, mock_make_api_call):
        """Test getting deployment status."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        mock_make_api_call.return_value = {"deploymentStatus": "Deployed"}

        result = get_deployments(access_token, metadata)

        mock_make_api_call.assert_called_once()
        assert isinstance(result, DeploymentsResponse)
        assert result.deploymentStatus == "Deployed"


class TestWaitForDeployment:
    @patch("datacustomcode.deploy.time.sleep")
    @patch("datacustomcode.deploy.time.time")
    @patch("datacustomcode.deploy.get_deployments")
    def test_wait_for_deployment_success(
        self, mock_get_deployments, mock_time, mock_sleep
    ):
        """Test waiting for deployment to complete successfully."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )
        callback = MagicMock()

        # Mock deployment statuses
        mock_time.side_effect = [100, 101, 102]  # Start time, check time, final time
        mock_get_deployments.return_value = DeploymentsResponse(
            deploymentStatus="Deployed"
        )

        wait_for_deployment(access_token, metadata, callback)

        # Verify the callback was called with the correct status
        callback.assert_called_once_with("Deployed")
        mock_sleep.assert_not_called()

    @patch("datacustomcode.deploy.time.sleep")
    @patch("datacustomcode.deploy.time.time")
    @patch("datacustomcode.deploy.get_deployments")
    def test_wait_for_deployment_timeout(
        self, mock_get_deployments, mock_time, mock_sleep
    ):
        """Test wait for deployment timing out."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        # Mock time to simulate timeout
        mock_time.side_effect = [100, 100 + 3001]  # Start time, check time (> timeout)
        mock_get_deployments.return_value = DeploymentsResponse(
            deploymentStatus="InProgress"
        )

        with pytest.raises(TimeoutError, match="Deployment timed out"):
            wait_for_deployment(access_token, metadata)


class TestDataTransformConfig:
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=(
            '{"sdkVersion": "1.0.0", "entryPoint": "entrypoint.py", '
            '"dataspace": "test_dataspace", '
            '"permissions": {"read": {"dlo": ["input_dlo"]}, '
            '"write": {"dlo": ["output_dlo"]}}}'
        ),
    )
    def test_get_data_transform_config(self, mock_file):
        """Test getting data transform config from config.json file."""
        result = get_data_transform_config("/test/dir")
        assert isinstance(result, DataTransformConfig)
        assert result.sdkVersion == "1.0.0"
        assert result.entryPoint == "entrypoint.py"
        assert result.dataspace == "test_dataspace"
        assert result.permissions.read.dlo == ["input_dlo"]
        assert result.permissions.write.dlo == ["output_dlo"]

    @patch("datacustomcode.deploy.os.path.exists")
    def test_verify_data_transform_config_missing(self, mock_exists):
        """Test verifying data transform config file when it doesn't exist."""
        mock_exists.return_value = False
        with pytest.raises(
            FileNotFoundError,
            match="config.json not found at /test/dir/payload/config.json",
        ):
            verify_data_transform_config("/test/dir/payload")

    @patch("datacustomcode.deploy.os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"invalid": "json"')
    def test_verify_data_transform_config_invalid_json(self, mock_file, mock_exists):
        """Test verifying data transform config with invalid JSON."""
        mock_exists.return_value = True
        with pytest.raises(
            ValueError,
            match="config.json at /test/dir/payload/config.json is not valid JSON",
        ):
            verify_data_transform_config("/test/dir/payload")

    @patch("datacustomcode.deploy.os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"sdkVersion": "1.0.0"}')
    def test_verify_data_transform_config_missing_fields(self, mock_file, mock_exists):
        """Test verifying data transform config with missing required fields."""
        mock_exists.return_value = True
        with pytest.raises(
            ValueError,
            match="config.json at /test/dir/payload/config.json is missing "
            "required fields: entryPoint, dataspace, permissions",
        ):
            verify_data_transform_config("/test/dir/payload")


class TestCreateDataTransform:
    @patch("datacustomcode.deploy.get_data_transform_config")
    @patch("datacustomcode.deploy._make_api_call")
    def test_create_data_transform(self, mock_make_api_call, mock_get_config):
        """Test creating a data transform in DataCloud."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        mock_get_config.return_value = DataTransformConfig(
            sdkVersion="1.0.0",
            entryPoint="entrypoint.py",
            dataspace="test_dataspace",
            permissions=Permissions(
                read=DloPermission(dlo=["input_dlo"]),
                write=DloPermission(dlo=["output_dlo"]),
            ),
        )
        mock_make_api_call.return_value = {"id": "transform_id"}

        result = create_data_transform("/test/dir", access_token, metadata)

        mock_get_config.assert_called_once_with("/test/dir")
        mock_make_api_call.assert_called_once()

        # Verify the request body structure
        request_body = mock_make_api_call.call_args[1]["json"]
        assert request_body["definition"]["type"] == "DCSQL"
        assert request_body["dataSpaceName"] == "test_dataspace"
        assert "nodes" in request_body["definition"]["manifest"]
        assert "sources" in request_body["definition"]["manifest"]
        assert "macros" in request_body["definition"]["manifest"]
        assert (
            request_body["definition"]["manifest"]["macros"]["macro.byoc"]["arguments"][
                0
            ]["name"]
            == "test_job"
        )

        assert result == {"id": "transform_id"}


class TestDeployFull:
    @patch("datacustomcode.deploy._retrieve_access_token")
    @patch("datacustomcode.deploy.prepare_dependency_archive")
    @patch("datacustomcode.deploy.verify_data_transform_config")
    @patch("datacustomcode.deploy.create_deployment")
    @patch("datacustomcode.deploy.zip_and_upload_directory")
    @patch("datacustomcode.deploy.wait_for_deployment")
    @patch("datacustomcode.deploy.create_data_transform")
    def test_deploy_full(
        self,
        mock_create_transform,
        mock_wait,
        mock_zip_upload,
        mock_create_deployment,
        mock_verify_config,
        mock_prepare,
        mock_retrieve_token,
    ):
        """Test full deployment process."""
        credentials = Credentials(
            username="user",
            password="pass",
            client_id="id",
            client_secret="secret",
            login_url="https://example.com",
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )
        callback = MagicMock()

        # Setup mocks
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        mock_retrieve_token.return_value = access_token
        mock_create_deployment.return_value = CreateDeploymentResponse(
            fileUploadUrl="https://upload.example.com"
        )

        # Call function
        result = deploy_full("/test/dir", metadata, credentials, callback)

        # Assertions
        mock_retrieve_token.assert_called_once_with(credentials)
        mock_prepare.assert_called_once_with("/test/dir")
        mock_verify_config.assert_called_once_with("/test/dir")
        mock_create_deployment.assert_called_once_with(access_token, metadata)
        mock_zip_upload.assert_called_once_with(
            "/test/dir", "https://upload.example.com"
        )
        mock_wait.assert_called_once_with(access_token, metadata, callback)
        mock_create_transform.assert_called_once_with(
            "/test/dir", access_token, metadata
        )
        assert result == access_token


class TestRunDataTransform:
    @patch("datacustomcode.deploy._make_api_call")
    def test_run_data_transform(self, mock_make_api_call):
        """Test running a data transform."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job", version="1.0.0", description="Test job"
        )

        mock_make_api_call.return_value = {"status": "Running"}

        result = run_data_transform(access_token, metadata)

        mock_make_api_call.assert_called_once()
        assert result == {"status": "Running"}
