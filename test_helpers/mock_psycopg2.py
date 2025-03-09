"""
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