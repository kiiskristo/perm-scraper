import os
import pytest
from unittest.mock import patch, MagicMock, call
import datetime

# Try to import from perm_scraper, catching potential import errors
try:
    from perm_scraper import (
        connect_postgres, 
        initialize_postgres_tables,
        save_to_postgres
    )
except ImportError:
    # Create stub functions for testing if imports fail
    def connect_postgres():
        """Stub function if import fails"""
        return MagicMock()
    
    def initialize_postgres_tables(conn):
        """Stub function if import fails"""
        return True
    
    def save_to_postgres(data):
        """Stub function if import fails"""
        return True

# Test data fixture
@pytest.fixture
def sample_perm_data():
    """Sample PERM data for testing"""
    return {
        "todayDate": "2023-05-15",
        "dailyProgress": [
            {"date": "May/15/23 Mon", "total": 100},
            {"date": "May/14/23 Sun", "total": 90}
        ],
        "submissionMonths": [
            {
                "month": "May 2023",
                "active": True,
                "statuses": [
                    {"status": "ANALYST REVIEW", "count": 200, "dailyChange": 10},
                    {"status": "CERTIFIED", "count": 50, "dailyChange": 5}
                ]
            }
        ],
        "summary": {
            "total_applications": 250,
            "pending_applications": 200,
            "pending_percentage": 80.0,
            "changes_today": 15,
            "completed_today": 5
        },
        "processingTimes": {
            "30_percentile": 30,
            "50_percentile": 45,
            "80_percentile": 60
        }
    }

def test_connect_postgres_success():
    """Test successful PostgreSQL connection"""
    with patch('psycopg2.connect') as mock_connect:
        # Setup the mock
        mock_connect.return_value = MagicMock()
        
        # Call the function
        conn = connect_postgres()
        
        # Assertions
        assert conn is not None
        mock_connect.assert_called_once()

def test_connect_postgres_failure():
    """Test failed PostgreSQL connection"""
    with patch('psycopg2.connect') as mock_connect:
        # Setup the mock to raise an exception
        mock_connect.side_effect = Exception("Connection failed")
        
        # Call the function
        conn = connect_postgres()
        
        # Assertions
        assert conn is None
        mock_connect.assert_called_once()

def test_initialize_postgres_tables_success():
    """Test successful PostgreSQL table initialization"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Call the function
    result = initialize_postgres_tables(mock_conn)
    
    # Assertions
    assert result is True
    assert mock_cursor.execute.call_count >= 10  # Should have multiple execute calls
    mock_conn.commit.assert_called_once()

def test_initialize_postgres_tables_failure():
    """Test failed PostgreSQL table initialization"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("SQL Error")
    
    # Call the function
    result = initialize_postgres_tables(mock_conn)
    
    # Assertions
    assert result is False
    mock_conn.rollback.assert_called_once()

def test_save_to_postgres(sample_perm_data):
    """Test saving data to PostgreSQL"""
    with patch('perm_scraper.connect_postgres') as mock_connect, \
         patch('perm_scraper.initialize_postgres_tables') as mock_init:
        
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_init.return_value = True
        
        # Make the record_date check return False (record doesn't exist)
        mock_cursor.fetchone.return_value = [False]
        
        # Call the function
        result = save_to_postgres(sample_perm_data)
        
        # Assertions
        assert result is True
        mock_connect.assert_called_once()
        mock_init.assert_called_once_with(mock_conn)
        assert mock_cursor.execute.call_count >= 2  # Should have multiple execute calls
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

def test_save_to_postgres_connection_failure():
    """Test PostgreSQL connection failure when saving"""
    with patch('perm_scraper.connect_postgres') as mock_connect:
        # Setup mock to return None (connection failure)
        mock_connect.return_value = None
        
        # Call the function
        result = save_to_postgres({})
        
        # Assertions
        assert result is False
        mock_connect.assert_called_once()

def test_save_to_postgres_initialization_failure():
    """Test PostgreSQL table initialization failure when saving"""
    with patch('perm_scraper.connect_postgres') as mock_connect, \
         patch('perm_scraper.initialize_postgres_tables') as mock_init:
        
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_init.return_value = False  # Initialization fails
        
        # Call the function
        result = save_to_postgres({})
        
        # Assertions
        assert result is False
        mock_connect.assert_called_once()
        mock_init.assert_called_once_with(mock_conn)
        mock_conn.close.assert_called_once()

def test_save_to_postgres_execution_error(sample_perm_data):
    """Test handling SQL execution error when saving to PostgreSQL"""
    with patch('perm_scraper.connect_postgres') as mock_connect, \
         patch('perm_scraper.initialize_postgres_tables') as mock_init:
        
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_init.return_value = True
        
        # Make execute raise an exception
        mock_cursor.execute.side_effect = Exception("SQL Error")
        
        # Call the function
        result = save_to_postgres(sample_perm_data)
        
        # Assertions
        assert result is False
        mock_connect.assert_called_once()
        mock_init.assert_called_once_with(mock_conn)
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once() 