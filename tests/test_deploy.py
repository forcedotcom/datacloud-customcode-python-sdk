"""Tests for the deploy module."""

from unittest.mock import (
    MagicMock,
    mock_open,
    patch,
)
import zipfile

import pytest
import requests

from datacustomcode.credentials import AuthType, Credentials
from datacustomcode.deploy import (
    DloPermission,
    Permissions,
    get_config,
)

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
        get_deployments,
        has_nonempty_requirements_file,
        prepare_dependency_archive,
        run_data_transform,
        upload_zip,
        wait_for_deployment,
        zip,
    )


class TestPrepareDependencyArchive:
    # Shared expected commands
    EXPECTED_DOCKER_IMAGES_CMD = (
        "docker images -q datacloud-custom-code-dependency-builder"
    )
    EXPECTED_BUILD_CMD = (
        "DOCKER_DEFAULT_PLATFORM=linux/amd64 docker build "
        "-t datacloud-custom-code-dependency-builder -f Dockerfile.dependencies . "
    )
    EXPECTED_DOCKER_RUN_CMD = (
        "DOCKER_DEFAULT_PLATFORM=linux/amd64 docker run --rm "
        "-v /tmp/test_dir:/workspace "
        "datacloud-custom-code-dependency-builder "
    )

    @patch("datacustomcode.deploy.cmd_output")
    @patch("datacustomcode.deploy.shutil.copy")
    @patch("datacustomcode.deploy.tempfile.TemporaryDirectory")
    @patch("datacustomcode.deploy.os.path.join")
    @patch("datacustomcode.deploy.os.makedirs")
    @patch("datacustomcode.deploy.docker_build_cmd")
    @patch("datacustomcode.deploy.docker_run_cmd")
    def test_prepare_dependency_archive_image_exists(
        self,
        mock_docker_run_cmd,
        mock_docker_build_cmd,
        mock_makedirs,
        mock_join,
        mock_temp_dir,
        mock_copy,
        mock_cmd_output,
    ):
        """Test prepare_dependency_archive when Docker image already exists."""
        # Mock the temporary directory context manager
        mock_temp_dir_instance = MagicMock()
        mock_temp_dir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_temp_dir_instance.__exit__.return_value = None
        mock_temp_dir.return_value = mock_temp_dir_instance

        # Mock cmd_output to return image ID (indicating image exists)
        mock_cmd_output.return_value = "abc123"

        # Mock os.path.join for archive path
        mock_join.return_value = "/tmp/test_dir/native_dependencies.tar.gz"

        # Mock the docker command functions
        mock_docker_build_cmd.return_value = "mock build command"
        mock_docker_run_cmd.return_value = "mock run command"

        prepare_dependency_archive("/test/dir", "default")

        # Verify docker images command was called
        mock_cmd_output.assert_any_call(self.EXPECTED_DOCKER_IMAGES_CMD)

        # Verify docker build command was not called (since image already exists)
        mock_docker_build_cmd.assert_not_called()

        # Verify files were copied to temp directory
        mock_copy.assert_any_call("requirements.txt", "/tmp/test_dir")
        mock_copy.assert_any_call("build_native_dependencies.sh", "/tmp/test_dir")

        # Verify docker run command was called
        mock_docker_run_cmd.assert_called_once_with("default", "/tmp/test_dir")
        mock_cmd_output.assert_any_call("mock run command")

        # Verify archives directory was created
        mock_makedirs.assert_called_once_with("payload/archives", exist_ok=True)

        # Verify archive was copied back
        mock_copy.assert_any_call(
            "/tmp/test_dir/native_dependencies.tar.gz",
            "payload/archives/native_dependencies.tar.gz",
        )

    @patch("datacustomcode.deploy.cmd_output")
    @patch("datacustomcode.deploy.shutil.copy")
    @patch("datacustomcode.deploy.tempfile.TemporaryDirectory")
    @patch("datacustomcode.deploy.os.path.join")
    @patch("datacustomcode.deploy.os.makedirs")
    @patch("datacustomcode.deploy.docker_build_cmd")
    @patch("datacustomcode.deploy.docker_run_cmd")
    def test_prepare_dependency_archive_build_image(
        self,
        mock_docker_run_cmd,
        mock_docker_build_cmd,
        mock_makedirs,
        mock_join,
        mock_temp_dir,
        mock_copy,
        mock_cmd_output,
    ):
        """Test prepare_dependency_archive when Docker image needs to be built."""
        # Mock the temporary directory context manager
        mock_temp_dir_instance = MagicMock()
        mock_temp_dir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_temp_dir_instance.__exit__.return_value = None
        mock_temp_dir.return_value = mock_temp_dir_instance

        # Mock cmd_output to return None for image check (image doesn't exist)
        # and then return some value for subsequent calls
        mock_cmd_output.side_effect = [None, None, None, None]

        # Mock os.path.join for archive path
        mock_join.return_value = "/tmp/test_dir/native_dependencies.tar.gz"

        # Mock the docker command functions
        mock_docker_build_cmd.return_value = "mock build command"
        mock_docker_run_cmd.return_value = "mock run command"

        prepare_dependency_archive("/test/dir", "default")

        # Verify docker images command was called
        mock_cmd_output.assert_any_call(self.EXPECTED_DOCKER_IMAGES_CMD)

        # Verify docker build command was called
        mock_docker_build_cmd.assert_called_once_with("default")
        mock_cmd_output.assert_any_call("mock build command")

        # Verify files were copied to temp directory
        mock_copy.assert_any_call("requirements.txt", "/tmp/test_dir")
        mock_copy.assert_any_call("build_native_dependencies.sh", "/tmp/test_dir")

        # Verify docker run command was called
        mock_docker_run_cmd.assert_called_once_with("default", "/tmp/test_dir")
        mock_cmd_output.assert_any_call("mock run command")

        # Verify archives directory was created
        mock_makedirs.assert_called_once_with("payload/archives", exist_ok=True)

        # Verify archive was copied back
        mock_copy.assert_any_call(
            "/tmp/test_dir/native_dependencies.tar.gz",
            "payload/archives/native_dependencies.tar.gz",
        )

    @patch("datacustomcode.deploy.cmd_output")
    @patch("datacustomcode.deploy.shutil.copy")
    @patch("datacustomcode.deploy.tempfile.TemporaryDirectory")
    @patch("datacustomcode.deploy.os.path.join")
    @patch("datacustomcode.deploy.os.makedirs")
    @patch("datacustomcode.deploy.docker_build_cmd")
    @patch("datacustomcode.deploy.docker_run_cmd")
    def test_prepare_dependency_archive_docker_build_failure(
        self,
        mock_docker_run_cmd,
        mock_docker_build_cmd,
        mock_makedirs,
        mock_join,
        mock_temp_dir,
        mock_copy,
        mock_cmd_output,
    ):
        """Test prepare_dependency_archive when Docker build fails."""
        # Mock the temporary directory context manager
        mock_temp_dir_instance = MagicMock()
        mock_temp_dir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_temp_dir_instance.__exit__.return_value = None
        mock_temp_dir.return_value = mock_temp_dir_instance

        # Mock cmd_output to return None for image check, then raise exception for build
        from datacustomcode.cmd import CalledProcessError

        mock_cmd_output.side_effect = [
            None,  # Image doesn't exist
            CalledProcessError(
                1, ("docker", "build"), b"Build failed", b"Error"
            ),  # Build fails
        ]

        with pytest.raises(CalledProcessError, match="Build failed"):
            prepare_dependency_archive("/test/dir", "default")

        # Verify docker images command was called
        mock_cmd_output.assert_any_call(self.EXPECTED_DOCKER_IMAGES_CMD)

        # Verify docker build command was called
        mock_docker_build_cmd.assert_called_once_with("default")

    @patch("datacustomcode.deploy.cmd_output")
    @patch("datacustomcode.deploy.shutil.copy")
    @patch("datacustomcode.deploy.tempfile.TemporaryDirectory")
    @patch("datacustomcode.deploy.os.path.join")
    @patch("datacustomcode.deploy.os.makedirs")
    @patch("datacustomcode.deploy.docker_build_cmd")
    @patch("datacustomcode.deploy.docker_run_cmd")
    def test_prepare_dependency_archive_docker_run_failure(
        self,
        mock_docker_run_cmd,
        mock_docker_build_cmd,
        mock_makedirs,
        mock_join,
        mock_temp_dir,
        mock_copy,
        mock_cmd_output,
    ):
        """Test prepare_dependency_archive when Docker run fails."""
        # Mock the temporary directory context manager
        mock_temp_dir_instance = MagicMock()
        mock_temp_dir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_temp_dir_instance.__exit__.return_value = None
        mock_temp_dir.return_value = mock_temp_dir_instance

        # Mock cmd_output to return image ID, then raise exception for run
        from datacustomcode.cmd import CalledProcessError

        mock_cmd_output.side_effect = [
            "abc123",  # Image exists
            CalledProcessError(
                1, ("docker", "run"), b"Run failed", b"Error"
            ),  # Run fails
        ]

        with pytest.raises(CalledProcessError, match="Run failed"):
            prepare_dependency_archive("/test/dir", "default")

        # Verify docker images command was called
        mock_cmd_output.assert_any_call(self.EXPECTED_DOCKER_IMAGES_CMD)

        # Verify files were copied to temp directory
        mock_copy.assert_any_call("requirements.txt", "/tmp/test_dir")
        mock_copy.assert_any_call("build_native_dependencies.sh", "/tmp/test_dir")

        # Verify docker run command was called
        mock_docker_run_cmd.assert_called_once_with("default", "/tmp/test_dir")

    @patch("datacustomcode.deploy.cmd_output")
    @patch("datacustomcode.deploy.shutil.copy")
    @patch("datacustomcode.deploy.tempfile.TemporaryDirectory")
    @patch("datacustomcode.deploy.os.path.join")
    @patch("datacustomcode.deploy.os.makedirs")
    @patch("datacustomcode.deploy.docker_build_cmd")
    @patch("datacustomcode.deploy.docker_run_cmd")
    def test_prepare_dependency_archive_file_copy_failure(
        self,
        mock_docker_run_cmd,
        mock_docker_build_cmd,
        mock_makedirs,
        mock_join,
        mock_temp_dir,
        mock_copy,
        mock_cmd_output,
    ):
        """Test prepare_dependency_archive when file copy fails."""
        # Mock the temporary directory context manager
        mock_temp_dir_instance = MagicMock()
        mock_temp_dir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_temp_dir_instance.__exit__.return_value = None
        mock_temp_dir.return_value = mock_temp_dir_instance

        # Mock cmd_output to return image ID
        mock_cmd_output.return_value = "abc123"

        # Mock shutil.copy to raise exception
        mock_copy.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError, match="File not found"):
            prepare_dependency_archive("/test/dir", "default")

        # Verify docker images command was called
        mock_cmd_output.assert_any_call(self.EXPECTED_DOCKER_IMAGES_CMD)

        # Verify files were attempted to be copied
        mock_copy.assert_any_call("requirements.txt", "/tmp/test_dir")


class TestHasNonemptyRequirementsFile:
    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="numpy==1.21.0\npandas==1.3.0",
    )
    def test_has_nonempty_requirements_file_with_dependencies(
        self, mock_file, mock_isfile, mock_dirname
    ):
        """
        Test has_nonempty_requirements_file when requirements.txt has dependencies.
        """
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = True

        result = has_nonempty_requirements_file("/test/dir")

        assert result is True
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")
        mock_file.assert_called_once_with(
            "/parent/dir/requirements.txt", "r", encoding="utf-8"
        )

    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="# This is a comment\n\n  # Another comment",
    )
    def test_has_nonempty_requirements_file_only_comments(
        self, mock_file, mock_isfile, mock_dirname
    ):
        """
        Test has_nonempty_requirements_file when requirements.txt has only comments.
        """
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = True

        result = has_nonempty_requirements_file("/test/dir")

        assert result is False
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")
        mock_file.assert_called_once_with(
            "/parent/dir/requirements.txt", "r", encoding="utf-8"
        )

    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_has_nonempty_requirements_file_empty_file(
        self, mock_file, mock_isfile, mock_dirname
    ):
        """Test has_nonempty_requirements_file when requirements.txt is empty."""
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = True

        result = has_nonempty_requirements_file("/test/dir")

        assert result is False
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")
        mock_file.assert_called_once_with(
            "/parent/dir/requirements.txt", "r", encoding="utf-8"
        )

    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    def test_has_nonempty_requirements_file_not_exists(self, mock_isfile, mock_dirname):
        """Test has_nonempty_requirements_file when requirements.txt doesn't exist."""
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = False

        result = has_nonempty_requirements_file("/test/dir")

        assert result is False
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")

    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    @patch("builtins.open", side_effect=PermissionError("Permission denied"))
    def test_has_nonempty_requirements_file_permission_error(
        self, mock_file, mock_isfile, mock_dirname
    ):
        """Test has_nonempty_requirements_file when file access fails."""
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = True

        result = has_nonempty_requirements_file("/test/dir")

        assert result is False
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")
        mock_file.assert_called_once_with(
            "/parent/dir/requirements.txt", "r", encoding="utf-8"
        )

    @patch("datacustomcode.deploy.os.path.dirname")
    @patch("datacustomcode.deploy.os.path.isfile")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="numpy==1.21.0\n# Comment\npandas==1.3.0",
    )
    def test_has_nonempty_requirements_file_mixed_content(
        self, mock_file, mock_isfile, mock_dirname
    ):
        """Test has_nonempty_requirements_file with mixed dependencies and comments."""
        mock_dirname.return_value = "/parent/dir"
        mock_isfile.return_value = True

        result = has_nonempty_requirements_file("/test/dir")

        assert result is True
        mock_isfile.assert_called_once_with("/parent/dir/requirements.txt")
        mock_file.assert_called_once_with(
            "/parent/dir/requirements.txt", "r", encoding="utf-8"
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
            login_url="https://example.com",
            client_id="id",
            auth_type=AuthType.USERNAME_PASSWORD,
            username="user",
            password="pass",
            client_secret="secret",
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
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
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
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
        )

        # Mock HTTP error with 409 Conflict
        mock_response = MagicMock()
        mock_response.status_code = 409
        http_error = requests.HTTPError("Deployment exists")
        http_error.response = mock_response
        mock_make_api_call.side_effect = http_error

        with pytest.raises(ValueError, match="Deployment test_job exists"):
            create_deployment(access_token, metadata)


class TestZip:
    @patch("datacustomcode.deploy.has_nonempty_requirements_file")
    @patch("datacustomcode.deploy.prepare_dependency_archive")
    @patch("zipfile.ZipFile")
    @patch("os.walk")
    def test_zip_with_requirements(
        self, mock_walk, mock_zipfile, mock_prepare, mock_has_requirements
    ):
        """Test zipping a directory with requirements.txt."""
        mock_has_requirements.return_value = True
        mock_zipfile_instance = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zipfile_instance
        mock_zipfile_instance.write = MagicMock()

        # Mock os.walk to return some test files
        mock_walk.return_value = [
            ("/test/dir", ["subdir"], ["file1.py", "file2.py"]),
            ("/test/dir/subdir", [], ["file3.py"]),
        ]

        zip("/test/dir", "default")

        mock_has_requirements.assert_called_once_with("/test/dir")
        mock_prepare.assert_called_once_with("/test/dir", "default")
        mock_zipfile.assert_called_once_with(
            "deployment.zip", "w", zipfile.ZIP_DEFLATED
        )
        assert mock_zipfile_instance.write.call_count == 3  # One call per file

    @patch("datacustomcode.deploy.has_nonempty_requirements_file")
    @patch("datacustomcode.deploy.prepare_dependency_archive")
    @patch("zipfile.ZipFile")
    @patch("os.walk")
    def test_zip_without_requirements(
        self, mock_walk, mock_zipfile, mock_prepare, mock_has_requirements
    ):
        """Test zipping a directory without requirements.txt."""
        mock_has_requirements.return_value = False
        mock_zipfile_instance = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zipfile_instance
        mock_zipfile_instance.write = MagicMock()

        # Mock os.walk to return some test files
        mock_walk.return_value = [
            ("/test/dir", ["subdir"], ["file1.py", "file2.py"]),
            ("/test/dir/subdir", [], ["file3.py"]),
        ]

        zip("/test/dir", "default")

        mock_has_requirements.assert_called_once_with("/test/dir")
        mock_prepare.assert_not_called()
        mock_zipfile.assert_called_once_with(
            "deployment.zip", "w", zipfile.ZIP_DEFLATED
        )
        assert mock_zipfile_instance.write.call_count == 3  # One call per file


class TestUploadZip:
    @patch("datacustomcode.deploy.requests.put")
    @patch("builtins.open", new_callable=mock_open, read_data=b"test data")
    def test_upload_zip_success(self, mock_file, mock_put):
        """Test successful zip upload."""
        mock_response = MagicMock()
        mock_put.return_value = mock_response

        upload_zip("https://upload.example.com")

        mock_file.assert_called_once_with("deployment.zip", "rb")
        mock_put.assert_called_once_with(
            "https://upload.example.com",
            data=mock_file.return_value,
            headers={"Content-Type": "application/zip"},
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("datacustomcode.deploy.requests.put")
    @patch("builtins.open", new_callable=mock_open, read_data=b"test data")
    def test_upload_zip_http_error(self, mock_file, mock_put):
        """Test zip upload with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("Upload failed")
        mock_put.return_value = mock_response

        with pytest.raises(requests.HTTPError, match="Upload failed"):
            upload_zip("https://upload.example.com")

        mock_file.assert_called_once_with("deployment.zip", "rb")
        mock_put.assert_called_once_with(
            "https://upload.example.com",
            data=mock_file.return_value,
            headers={"Content-Type": "application/zip"},
        )


class TestGetDeployments:
    @patch("datacustomcode.deploy._make_api_call")
    def test_get_deployments(self, mock_make_api_call):
        """Test getting deployment status."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
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
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
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
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
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
    def test_get_config(self, mock_file):
        """Test getting data transform config from config.json file."""
        result = get_config("/test/dir")
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
            get_config("/test/dir/payload")

    @patch("datacustomcode.deploy.os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"invalid": "json"')
    def test_verify_data_transform_config_invalid_json(self, mock_file, mock_exists):
        """Test verifying data transform config with invalid JSON."""
        mock_exists.return_value = True
        with pytest.raises(
            ValueError,
            match="config.json at /test/dir/payload/config.json is not valid JSON",
        ):
            get_config("/test/dir/payload")

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
            get_config("/test/dir/payload")


class TestCreateDataTransform:
    @patch("datacustomcode.deploy.get_config")
    @patch("datacustomcode.deploy._make_api_call")
    def test_create_data_transform(self, mock_make_api_call, mock_get_config):
        """Test creating a data transform in DataCloud."""
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        metadata = TransformationJobMetadata(
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
        )

        data_transform_config = DataTransformConfig(
            sdkVersion="1.0.0",
            entryPoint="entrypoint.py",
            dataspace="test_dataspace",
            permissions=Permissions(
                read=DloPermission(dlo=["input_dlo"]),
                write=DloPermission(dlo=["output_dlo"]),
            ),
        )
        mock_make_api_call.return_value = {"id": "transform_id"}

        result = create_data_transform(
            "/test/dir", access_token, metadata, data_transform_config
        )

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
    @patch("datacustomcode.deploy.get_config")
    @patch("datacustomcode.deploy.create_data_transform")
    @patch("datacustomcode.deploy.wait_for_deployment")
    @patch("datacustomcode.deploy.upload_zip")
    @patch("datacustomcode.deploy.zip")
    @patch("datacustomcode.deploy.create_deployment")
    @patch("datacustomcode.deploy._retrieve_access_token")
    def test_deploy_full(
        self,
        mock_retrieve_token,
        mock_create_deployment,
        mock_zip,
        mock_upload_zip,
        mock_wait,
        mock_create_transform,
        mock_get_config,
    ):
        """Test full deployment process."""
        data_transform_config = DataTransformConfig(
            sdkVersion="1.0.0",
            entryPoint="entrypoint.py",
            dataspace="test_dataspace",
            permissions=Permissions(
                read=DloPermission(dlo=["input_dlo"]),
                write=DloPermission(dlo=["output_dlo"]),
            ),
        )
        mock_get_config.return_value = data_transform_config
        credentials = Credentials(
            login_url="https://example.com",
            client_id="id",
            auth_type=AuthType.USERNAME_PASSWORD,
            username="user",
            password="pass",
            client_secret="secret",
        )
        metadata = TransformationJobMetadata(
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
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
        result = deploy_full("/test/dir", metadata, credentials, "default", callback)

        # Assertions
        mock_retrieve_token.assert_called_once_with(credentials)
        mock_get_config.assert_called_once_with("/test/dir")
        mock_create_deployment.assert_called_once_with(access_token, metadata)
        mock_zip.assert_called_once_with("/test/dir", "default")
        mock_upload_zip.assert_called_once_with("https://upload.example.com")
        mock_wait.assert_called_once_with(access_token, metadata, callback)
        mock_create_transform.assert_called_once_with(
            "/test/dir", access_token, metadata, data_transform_config
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
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
        )

        mock_make_api_call.return_value = {"status": "Running"}

        result = run_data_transform(access_token, metadata)

        mock_make_api_call.assert_called_once()
        assert result == {"status": "Running"}


class TestDeployFullWithDockerIntegration:
    @patch("datacustomcode.deploy._retrieve_access_token")
    @patch("datacustomcode.deploy.create_deployment")
    @patch("datacustomcode.deploy.zip")
    @patch("datacustomcode.deploy.upload_zip")
    @patch("datacustomcode.deploy.wait_for_deployment")
    @patch("datacustomcode.deploy.create_data_transform")
    @patch("datacustomcode.deploy.get_config")
    @patch("datacustomcode.deploy.has_nonempty_requirements_file")
    def test_deploy_full_happy_path(
        self,
        mock_has_requirements,
        mock_get_config,
        mock_create_transform,
        mock_wait,
        mock_upload_zip,
        mock_zip,
        mock_create_deployment,
        mock_retrieve_token,
    ):
        """Test full deployment process with Docker dependency building."""
        credentials = Credentials(
            login_url="https://example.com",
            client_id="id",
            auth_type=AuthType.USERNAME_PASSWORD,
            username="user",
            password="pass",
            client_secret="secret",
        )
        metadata = TransformationJobMetadata(
            name="test_job",
            version="1.0.0",
            description="Test job",
            computeType="CPU_M",
        )
        callback = MagicMock()

        data_transform_config = DataTransformConfig(
            sdkVersion="1.0.0",
            entryPoint="entrypoint.py",
            dataspace="test_dataspace",
            permissions=Permissions(
                read=DloPermission(dlo=["input_dlo"]),
                write=DloPermission(dlo=["output_dlo"]),
            ),
        )
        mock_get_config.return_value = data_transform_config
        # Setup mocks
        access_token = AccessTokenResponse(
            access_token="test_token", instance_url="https://instance.example.com"
        )
        mock_retrieve_token.return_value = access_token
        mock_create_deployment.return_value = CreateDeploymentResponse(
            fileUploadUrl="https://upload.example.com"
        )

        # Mock that requirements.txt exists and has dependencies
        mock_has_requirements.return_value = True
        data_transform_config = DataTransformConfig(
            sdkVersion="1.0.0",
            entryPoint="entrypoint.py",
            dataspace="test_dataspace",
            permissions=Permissions(
                read=DloPermission(dlo=["input_dlo"]),
                write=DloPermission(dlo=["output_dlo"]),
            ),
        )
        # Call function
        result = deploy_full("/test/dir", metadata, credentials, "default", callback)

        # Assertions
        mock_retrieve_token.assert_called_once_with(credentials)
        mock_get_config.assert_called_once_with("/test/dir")
        mock_create_deployment.assert_called_once_with(access_token, metadata)
        mock_zip.assert_called_once_with("/test/dir", "default")
        mock_upload_zip.assert_called_once_with("https://upload.example.com")
        mock_wait.assert_called_once_with(access_token, metadata, callback)
        mock_create_transform.assert_called_once_with(
            "/test/dir", access_token, metadata, data_transform_config
        )
        assert result == access_token
