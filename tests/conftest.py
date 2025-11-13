"""Pytest configuration and shared fixtures."""

from pathlib import Path
from unittest.mock import Mock

import pytest

# Add common fixtures here that can be shared across test modules


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    return tmp_path


@pytest.fixture
def mock_path():
    """Create a mock Path object."""
    path = Mock(spec=Path)
    path.exists.return_value = True
    path.is_file.return_value = True
    path.name = "test_config.yaml"
    return path
