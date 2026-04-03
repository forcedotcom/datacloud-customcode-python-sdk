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

"""Tests for datacustomcode.ai.llm module."""

import pytest

from datacustomcode.ai import llm_complete


class TestLlmComplete:
    """Tests for llm_complete function."""

    def test_invalid_prompt_col_type_int(self):
        """Test that invalid prompt_col type raises TypeError."""
        with pytest.raises(TypeError, match="prompt_col must be a Column or str"):
            llm_complete(123)

    def test_invalid_max_tokens_type_string(self):
        """Test that string max_tokens raises ValueError."""
        with pytest.raises(ValueError, match="max_tokens must be a positive integer"):
            llm_complete("test_col", max_tokens="invalid")

    def test_invalid_max_tokens_type_float(self):
        """Test that float max_tokens raises ValueError."""
        with pytest.raises(ValueError, match="max_tokens must be a positive integer"):
            llm_complete("test_col", max_tokens=100.5)

    def test_negative_max_tokens(self):
        """Test that negative max_tokens raises ValueError."""
        with pytest.raises(ValueError, match="max_tokens must be a positive integer"):
            llm_complete("test_col", max_tokens=-1)

    def test_zero_max_tokens(self):
        """Test that zero max_tokens raises ValueError."""
        with pytest.raises(ValueError, match="max_tokens must be a positive integer"):
            llm_complete("test_col", max_tokens=0)


