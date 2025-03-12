import pytest
from unittest.mock import patch, MagicMock
import requests

def test_postgres_connection_error():
    """Test handling of PostgreSQL connection errors"""
    with patch('psycopg2.connect') as mock_connect:
        mock_connect.side_effect = Exception("Test connection error")
        
        from perm_scraper import connect_postgres
        result = connect_postgres()
        
        assert result is None

def test_html_fetch_retry():
    """Test retrying logic for HTTP errors"""
    # Create a better mock for the get request
    response_mock = MagicMock()
    response_mock.text = "<html>Success</html>"
    response_mock.raise_for_status.return_value = None

    # Use a better way to mock side_effect with a function
    def side_effect(*args, **kwargs):
        side_effect.counter += 1
        if side_effect.counter < 3:
            raise requests.exceptions.RequestException("Failed request")
        return response_mock
    side_effect.counter = 0
    
    with patch('time.sleep', return_value=None), \
         patch('requests.get', side_effect=side_effect):
        
        from perm_scraper import fetch_html_from_url
        html = fetch_html_from_url("https://example.com", debug=True, retry_count=3)
        
        assert html == "<html>Success</html>"

def test_html_fetch_failure():
    """Test handling of failed HTTP requests"""
    with patch('requests.get') as mock_get, \
         patch('time.sleep', return_value=None):
        # All attempts fail
        mock_get.side_effect = Exception("Connection error")
        
        from perm_scraper import fetch_html_from_url
        
        with pytest.raises(Exception):
            fetch_html_from_url("https://example.com", debug=True, retry_count=2) 