import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add the project root to the Python path to allow imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Create test_helpers directory if it doesn't exist
test_helpers_dir = os.path.join(os.path.dirname(__file__), 'test_helpers')
os.makedirs(test_helpers_dir, exist_ok=True)

# Check if psycopg2 is available, if not use our mock
try:
    import psycopg2
except (ImportError, ModuleNotFoundError):
    # Create the mock_psycopg2.py file if it doesn't exist
    mock_file_path = os.path.join(test_helpers_dir, 'mock_psycopg2.py')
    if not os.path.exists(mock_file_path):
        with open(mock_file_path, 'w') as f:
            f.write('''"""
Mock PostgreSQL module for testing without requiring actual PostgreSQL libraries.
This allows tests to run in environments without PostgreSQL installed.
"""

import sys
from unittest.mock import MagicMock

# Create mock classes
class MockConnection(MagicMock):
    def cursor(self):
        return MockCursor()
    
    def close(self):
        pass
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

class MockCursor(MagicMock):
    def execute(self, query, params=None):
        return None
    
    def executemany(self, query, params_list):
        return None
    
    def fetchone(self):
        return [False]
    
    def fetchall(self):
        return []
    
    def close(self):
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

# Mock psycopg2 module
mock_psycopg2 = MagicMock()
mock_psycopg2.connect = MagicMock(return_value=MockConnection())
mock_psycopg2.Error = Exception
mock_psycopg2.DatabaseError = Exception
mock_psycopg2.IntegrityError = Exception

# Mock extras module
mock_extras = MagicMock()
mock_extras.execute_batch = MagicMock(side_effect=lambda cur, query, data: None)
mock_psycopg2.extras = mock_extras

# Create a function to install the mock
def install_mock():
    """Install the mock psycopg2 module"""
    sys.modules['psycopg2'] = mock_psycopg2
    sys.modules['psycopg2.extras'] = mock_extras
''')
    
    # Import and install the mock
    from test_helpers.mock_psycopg2 import install_mock
    install_mock()


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

# Mock PostgreSQL for all tests to avoid needing a real PostgreSQL connection
@pytest.fixture(autouse=True)
def mock_postgres_connection():
    # Use different approach depending on whether we're using real or mock psycopg2
    if 'psycopg2' in sys.modules and not isinstance(sys.modules['psycopg2'], MagicMock):
        # Real psycopg2 is available, so patch it
        with patch('psycopg2.connect') as mock_connect:
            # Create mock cursor and connection
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            
            # Setup cursor methods
            mock_cursor.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = [False]  # For "EXISTS" queries
            
            # Setup connection methods
            mock_connection.cursor.return_value = mock_cursor
            mock_connection.__enter__.return_value = mock_connection
            
            # Make connect return our mocked connection
            mock_connect.return_value = mock_connection
            
            yield mock_connect
    else:
        # We're already using the mock psycopg2, no need to patch
        yield sys.modules['psycopg2'].connect