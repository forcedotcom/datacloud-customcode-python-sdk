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

import json
import os
import shutil
import sys
import tempfile
import textwrap

import pytest

from datacustomcode import function_utils


@pytest.fixture
def sample_entrypoint():
    """Create a temporary entrypoint file with a function."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False
    ) as temp_file:
        entrypoint_content = textwrap.dedent(
            """
            from typing import List
            from pydantic import BaseModel

            class SampleRequest(BaseModel):
                message: str
                count: int = 5
                tags: List[str] = []
                version: str = "v1"

            class SampleResponse(BaseModel):
                result: str
                success: bool = True

            def function(request: SampleRequest) -> SampleResponse:
                return SampleResponse(result=f"Processed {request.message}")
            """
        )
        temp_file.write(entrypoint_content)
        temp_file_path = temp_file.name

    yield temp_file_path

    if os.path.exists(temp_file_path):
        os.unlink(temp_file_path)


@pytest.fixture
def entrypoint_no_annotations():
    """Create an entrypoint with no type annotations."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False
    ) as temp_file:
        entrypoint_content = textwrap.dedent(
            """
            def function(request):
                return {"result": "no annotations"}
            """
        )
        temp_file.write(entrypoint_content)
        temp_file_path = temp_file.name

    yield temp_file_path

    if os.path.exists(temp_file_path):
        os.unlink(temp_file_path)


def test_get_function_signature_types(sample_entrypoint, entrypoint_no_annotations):
    """Test extracting request and response types from function signatures."""
    module = function_utils.load_function_module(sample_entrypoint)
    func = function_utils.get_function_callable(module)
    req_type, resp_type, req_name, resp_name = (
        function_utils.get_function_signature_types(func)
    )

    assert req_name == "SampleRequest"
    assert resp_name == "SampleResponse"
    assert req_type is not None
    assert resp_type is not None

    module_no_annot = function_utils.load_function_module(entrypoint_no_annotations)
    func_no_annot = function_utils.get_function_callable(module_no_annot)
    req_type, resp_type, req_name, resp_name = (
        function_utils.get_function_signature_types(func_no_annot)
    )

    assert req_name is None
    assert resp_name is None


def test_inspect_function_types_static(sample_entrypoint, entrypoint_no_annotations):
    """Test static AST-based inspection of function types."""
    req_name, resp_name = function_utils.inspect_function_types_static(
        sample_entrypoint
    )
    assert req_name == "SampleRequest"
    assert resp_name == "SampleResponse"

    req_name, resp_name = function_utils.inspect_function_types_static(
        entrypoint_no_annotations
    )
    assert req_name is None
    assert resp_name is None

def test_inspect_function_types(sample_entrypoint):
    """Test dynamic inspection of function types."""
    req_name, resp_name = function_utils.inspect_function_types(sample_entrypoint)
    assert req_name == "SampleRequest"
    assert resp_name == "SampleResponse"

    req_name, resp_name = function_utils.inspect_function_types("/nonexistent/file.py")
    assert req_name is None
    assert resp_name is None


def test_get_request_type(sample_entrypoint, entrypoint_no_annotations):
    """Test getting request type from entrypoint."""
    req_type = function_utils.get_request_type(sample_entrypoint)
    assert req_type is not None
    assert hasattr(req_type, "model_fields")

    with pytest.raises(ValueError, match="must have a type annotation"):
        function_utils.get_request_type(entrypoint_no_annotations)


def test_generate_test_json():
    """Test generating test.json file from entrypoint with simple and complex nested types."""
    temp_dir = tempfile.mkdtemp()
    models_file = os.path.join(temp_dir, "test_models.py")

    try:
        # Test 1: Simple request type
        entrypoint_simple = os.path.join(temp_dir, "entrypoint_simple.py")
        output_simple = os.path.join(temp_dir, "test_simple.json")

        with open(models_file, "w") as f:
            models_content = textwrap.dedent(
                """
                from pydantic import BaseModel
                from typing import List

                class SimpleRequest(BaseModel):
                    message: str
                    count: int = 5
                    tags: List[str] = []
                    version: str = "v1"

                class NestedConfig(BaseModel):
                    host: str
                    port: int = 8080
                    enabled: bool = True

                class ComplexRequest(BaseModel):
                    name: str
                    max_items: int = 100
                    config: NestedConfig
                    metadata: dict = {}
                """
            )
            f.write(models_content)

        with open(entrypoint_simple, "w") as f:
            entrypoint_content = textwrap.dedent(
                """
                from test_models import SimpleRequest

                def function(request: SimpleRequest):
                    return {"result": "ok"}
                """
            )
            f.write(entrypoint_content)

        sys.path.insert(0, temp_dir)

        function_utils.generate_test_json(entrypoint_simple, output_simple)
        assert os.path.exists(output_simple)

        with open(output_simple, "r") as f:
            data = json.load(f)

        assert "message" in data
        assert data["count"] == 5
        assert data["version"] == "v1"
        assert data["tags"] == []

        # Test 2: Complex request type with nested models
        entrypoint_complex = os.path.join(temp_dir, "entrypoint_complex.py")
        output_complex = os.path.join(temp_dir, "test_complex.json")

        with open(entrypoint_complex, "w") as f:
            entrypoint_content = textwrap.dedent(
                """
                from test_models import ComplexRequest

                def function(request: ComplexRequest):
                    return {"result": "ok"}
                """
            )
            f.write(entrypoint_content)

        function_utils.generate_test_json(entrypoint_complex, output_complex)
        assert os.path.exists(output_complex)

        with open(output_complex, "r") as f:
            complex_data = json.load(f)

        assert "name" in complex_data
        assert "max_items" in complex_data
        assert complex_data["max_items"] == 100
        assert "config" in complex_data
        assert isinstance(complex_data["config"], dict)
        assert "host" in complex_data["config"]
        assert "port" in complex_data["config"]
        assert complex_data["config"]["port"] == 8080
        assert complex_data["config"]["enabled"] is True
        assert "metadata" in complex_data
        assert complex_data["metadata"] == {}

    finally:
        if temp_dir in sys.path:
            sys.path.remove(temp_dir)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)