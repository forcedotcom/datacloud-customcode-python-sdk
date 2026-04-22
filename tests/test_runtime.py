from __future__ import annotations

import threading

import pytest

from datacustomcode.file.path.default import DefaultFindFilePath
from datacustomcode.function.runtime import Runtime
from datacustomcode.llm_gateway.default import DefaultLLMGateway


class TestRuntimeSingleton:
    """Test Runtime singleton pattern."""

    def setup_method(self):
        """Reset singleton before each test."""
        # Reset the singleton instance for each test
        Runtime._instance = None

    def test_second_instantiation_raises_error(self):
        """Test creating Runtime twice raises RuntimeError."""
        runtime1 = Runtime()
        assert runtime1 is not None

        with pytest.raises(RuntimeError) as exc_info:
            Runtime()

        assert "can only be instantiated once" in str(exc_info.value)
        assert "Do not instantiate it yourself" in str(exc_info.value)

    def test_concurrent_instantiation_thread_safe(self):
        """Test singleton is thread-safe during concurrent instantiation."""
        results = []
        errors = []

        def create_runtime():
            try:
                runtime = Runtime()
                results.append(runtime)
            except RuntimeError as e:
                errors.append(e)

        # Create 10 threads trying to instantiate Runtime
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_runtime)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Exactly one should succeed, rest should fail
        assert len(results) == 1, f"Expected 1 success, got {len(results)}"
        assert len(errors) == 9, f"Expected 9 errors, got {len(errors)}"

        # All errors should be RuntimeError about singleton
        for error in errors:
            assert "can only be instantiated once" in str(error)


class TestRuntimeProperties:
    """Test Runtime properties and methods."""

    def setup_method(self):
        """Reset singleton and create fresh instance for each test."""
        Runtime._instance = None
        self.runtime = Runtime()

    def test_runtime_has_llm_gateway(self):
        """Test Runtime has llm_gateway property."""
        assert hasattr(self.runtime, "llm_gateway")
        assert isinstance(self.runtime.llm_gateway, DefaultLLMGateway)

    def test_runtime_has_file(self):
        """Test Runtime has file property."""
        assert hasattr(self.runtime, "file")
        assert isinstance(self.runtime.file, DefaultFindFilePath)

    def test_runtime_initializes_only_once(self):
        """Test Runtime.__init__ prevents re-initialization."""
        # First init happens in setup_method
        llm_gateway_before = self.runtime.llm_gateway

        # Call __init__ again (shouldn't re-initialize)
        self.runtime.__init__()

        # Should still be the same instances
        llm_gateway_after = self.runtime.llm_gateway
        assert llm_gateway_before is llm_gateway_after
