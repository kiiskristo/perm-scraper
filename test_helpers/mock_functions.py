"""
Mock implementations of functions for testing
"""
from unittest.mock import MagicMock

def mock_save_to_postgres(data):
    """A completely mocked version of save_to_postgres that always returns True"""
    return True 