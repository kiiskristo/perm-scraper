import pytest
import os
import sys
from unittest.mock import patch

# Add the project root to the Python path to allow imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Mock MongoDB for all tests to avoid needing a real MongoDB connection
@pytest.fixture(autouse=True)
def mock_mongodb_connection():
    with patch('pymongo.MongoClient') as mock_client:
        # Set up the mock to return success for basic operations
        mock_db = mock_client.return_value.__getitem__.return_value
        mock_collection = mock_db.__getitem__.return_value
        mock_collection.insert_one.return_value.inserted_id = "test_id"
        
        # Simulate successful ping
        mock_client.return_value.admin.command.return_value = {"ok": 1}
        
        yield mock_client