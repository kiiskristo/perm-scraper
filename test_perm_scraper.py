import os
import json
import pytest
from unittest.mock import patch, MagicMock
from perm_scraper import extract_perm_data, fetch_html_from_url, save_to_mongodb

# Test data paths
MOCK_HTML_PATH = os.path.join(os.path.dirname(__file__), "mock_data", "perm_timeline.html")

@pytest.fixture
def mock_html():
    """Load mock HTML data for testing"""
    if not os.path.exists(MOCK_HTML_PATH):
        pytest.skip(f"Mock HTML file not found at {MOCK_HTML_PATH}")
    
    with open(MOCK_HTML_PATH, 'r', encoding='utf-8') as f:
        return f.read()

def test_extract_perm_data(mock_html):
    """Test that data extraction works with mock HTML"""
    # Extract data from mock HTML
    data = extract_perm_data(mock_html, debug=True)
    
    # Verify the basic structure of the returned data
    assert isinstance(data, dict), "Extracted data should be a dictionary"
    
    # Check for essential fields
    assert "submissionMonths" in data, "Data should contain submissionMonths"
    assert len(data["submissionMonths"]) > 0, "submissionMonths should not be empty"
    
    # Test summary calculations
    assert "summary" in data, "Data should contain summary statistics"
    assert "total_applications" in data["summary"], "Summary should contain total_applications"
    assert "pending_applications" in data["summary"], "Summary should contain pending_applications"
    assert "pending_percentage" in data["summary"], "Summary should contain pending_percentage"
    
    # Verify percentiles if available
    if "processingTimes" in data:
        assert "30_percentile" in data["processingTimes"], "processingTimes should contain 30_percentile"
        assert "50_percentile" in data["processingTimes"], "processingTimes should contain 50_percentile"
        assert "80_percentile" in data["processingTimes"], "processingTimes should contain 80_percentile"
    
    # Verify daily progress data if available
    if "dailyProgress" in data:
        assert len(data["dailyProgress"]) > 0, "dailyProgress should not be empty"
        
        # Check the structure of daily progress entries
        for entry in data["dailyProgress"]:
            assert "date" in entry, "Daily progress entry should contain date"
            assert "total" in entry, "Daily progress entry should contain total"

@patch('requests.get')
def test_fetch_html_from_url(mock_get, mock_html):
    """Test HTML fetching with mocked requests"""
    # Configure the mock response
    mock_response = MagicMock()
    mock_response.text = mock_html
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Call the function
    html = fetch_html_from_url("https://example.com", debug=True)
    
    # Verify the correct HTML was returned
    assert html == mock_html
    
    # Verify the request was made with the correct parameters
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert args[0] == "https://example.com"
    assert 'headers' in kwargs
    assert 'timeout' in kwargs

@patch('perm_scraper.connect_to_mongodb')
def test_save_to_mongodb(mock_connect):
    """Test saving data to MongoDB with a mock client"""
    # Create mock MongoDB client and collections
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_history_collection = MagicMock()
    
    # Set up the mocks with proper return values
    mock_connect.return_value = mock_client
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.side_effect = lambda x: mock_collection if x == 'test_collection' else mock_history_collection
    
    # Configure mock insert operations
    mock_collection.insert_one.return_value.inserted_id = "mock_id"
    mock_history_collection.insert_one.return_value.inserted_id = "history_id"
    
    # Sample data to save
    data = {
        "todayDate": "2025-03-05",
        "summary": {
            "total_applications": 253967,
            "pending_applications": 182707,
            "pending_percentage": 71.94,
            "changes_today": 469,
            "completed_today": 458
        },
        "processingTimes": {
            "30_percentile": 486,
            "50_percentile": 493,
            "80_percentile": 506
        }
    }
    
    # Mock environment setup
    with patch.dict('os.environ', {
        'MONGODB_URI': 'mongodb://localhost:27017',
        'MONGODB_DB': 'test_db',
        'MONGODB_COLLECTION': 'test_collection'
    }):
        # Call the function
        result = save_to_mongodb(data)
        
        # Verify the function returned success
        assert result is True
        
        # Verify connect_to_mongodb was called
        mock_connect.assert_called_once()
        
        # Verify the data was saved to the correct collections
        mock_collection.insert_one.assert_called_once()
        mock_history_collection.insert_one.assert_called_once()
        
        # Verify the history collection was also updated
        mock_db.__getitem__.assert_any_call("history")

def test_integration_extract_and_verify(mock_html):
    """Integration test to extract data and verify key metrics"""
    # Extract data from mock HTML
    data = extract_perm_data(mock_html, debug=True)
    
    # Verify specific metrics from the mock data
    summary = data.get("summary", {})
    
    # Check that our numbers match expected values from the mock data
    # These values should match what's in your mock HTML
    assert summary.get("total_applications") > 0, "Total applications should be positive"
    assert summary.get("pending_applications") > 0, "Pending applications should be positive"
    
    # Verify the pending percentage is calculated correctly
    expected_percentage = round(summary.get("pending_applications", 0) / 
                               summary.get("total_applications", 1) * 100, 2)
    assert abs(summary.get("pending_percentage", 0) - expected_percentage) < 0.01, \
        "Pending percentage calculation should be accurate"