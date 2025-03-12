import os
import pytest
from unittest.mock import patch, MagicMock
import tempfile
from perm_scraper import save_html_backup, initialize_postgres_tables

def test_save_html_backup():
    """Test HTML backup functionality"""
    html_content = "<html><body>Test content</body></html>"
    
    # Set environment for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["BACKUP_DIR"] = temp_dir
        
        # Run the function
        backup_path = save_html_backup(html_content, debug=True)
        
        # Verify results
        assert backup_path is not None
        assert os.path.exists(backup_path)
        
        # Check content
        with open(backup_path, 'r') as f:
            saved_content = f.read()
            assert saved_content == html_content

def test_initialize_postgres_tables():
    """Test PostgreSQL table initialization"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Call the function
    result = initialize_postgres_tables(mock_conn)
    
    # Assertions
    assert result is True
    # Should have executed several SQL statements
    assert mock_cursor.execute.call_count > 0
    mock_conn.commit.assert_called_once()

def test_initialize_postgres_tables_error():
    """Test PostgreSQL table initialization error handling"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("SQL Error")
    
    # Call the function
    result = initialize_postgres_tables(mock_conn)
    
    # Assertions
    assert result is False
    mock_conn.rollback.assert_called_once() 