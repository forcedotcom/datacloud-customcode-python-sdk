from __future__ import annotations

import filecmp
import os
import tempfile

import pytest

from datacustomcode.template import copy_template, template_dir


class TestTemplate:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    def test_template_dir_exists(self):
        """Test that the template directory exists."""
        assert os.path.isdir(template_dir)
        # Verify there are actual files in the template dir
        assert len(os.listdir(template_dir)) > 0

    def test_copy_template_basic(self, temp_dir):
        """Test basic functionality of copy_template."""
        # Call the function to copy templates to temporary directory
        copy_template(temp_dir)

        # Get the list of files in both directories
        template_items = os.listdir(template_dir)
        copied_items = os.listdir(temp_dir)

        # Verify all template items were copied
        for item in template_items:
            assert item in copied_items

            # Check if the copied item matches the original
            source_path = os.path.join(template_dir, item)
            dest_path = os.path.join(temp_dir, item)

            if os.path.isdir(source_path):
                # For directories, check they exist and have the same structure
                assert os.path.isdir(dest_path)

                # Compare directory contents
                source_files = sorted(
                    [
                        f
                        for f in os.listdir(source_path)
                        if os.path.isfile(os.path.join(source_path, f))
                    ]
                )
                dest_files = sorted(
                    [
                        f
                        for f in os.listdir(dest_path)
                        if os.path.isfile(os.path.join(dest_path, f))
                    ]
                )
                assert source_files == dest_files

                # Compare file contents for all files in the directory
                for file in source_files:
                    source_file = os.path.join(source_path, file)
                    dest_file = os.path.join(dest_path, file)
                    assert filecmp.cmp(
                        source_file, dest_file
                    ), f"File content differs: {file}"
            else:
                # For files, check content matches
                assert os.path.isfile(dest_path)
                assert filecmp.cmp(
                    source_path, dest_path
                ), f"File content differs: {item}"

    def test_copy_template_creates_nonexistent_dir(self, temp_dir):
        """Test copy_template creates directories that don't exist."""
        nonexistent_dir = os.path.join(temp_dir, "new_dir")

        # Verify directory doesn't exist yet
        assert not os.path.exists(nonexistent_dir)

        # Copy templates
        copy_template(nonexistent_dir)

        # Verify directory was created
        assert os.path.isdir(nonexistent_dir)

        # Verify contents match the template directory
        template_items = set(os.listdir(template_dir))
        copied_items = set(os.listdir(nonexistent_dir))
        assert template_items == copied_items

    def test_copy_template_with_existing_content(self, temp_dir):
        """Test copy_template preserves existing content."""
        # Create a test file in the target directory
        test_file = os.path.join(temp_dir, "existing_file.txt")
        with open(test_file, "w") as f:
            f.write("This is an existing file")

        # Copy templates
        copy_template(temp_dir)

        # Verify the existing file is still there
        assert os.path.exists(test_file)

        # Read the content to verify it wasn't changed
        with open(test_file, "r") as f:
            content = f.read()
        assert content == "This is an existing file"

        # Verify all template items were also copied
        template_items = os.listdir(template_dir)
        for item in template_items:
            assert os.path.exists(os.path.join(temp_dir, item))
