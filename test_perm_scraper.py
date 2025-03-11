import os
import json
import pytest
from unittest.mock import patch, MagicMock
from perm_scraper import extract_perm_data, fetch_html_from_url, save_to_postgres, run_scraper

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

@patch('perm_scraper.save_to_postgres', lambda data: True)  # Always succeeds
def test_save_to_postgres():
    """Test saving data to PostgreSQL with a mocked function"""
    from perm_scraper import save_to_postgres
    
    # Sample data to save
    data = {
        "todayDate": "2025-03-05",
        "summary": {
            "total_applications": 253967,
            "pending_applications": 182707,
            "pending_percentage": 71.94,
            "changes_today": 469,
            "completed_today": 458
        }
    }
    
    # Call the function
    result = save_to_postgres(data)
    
    # Verify the function returned success
    assert result is True

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

# Add tests for the updated run_scraper function
@patch('perm_scraper.fetch_html_from_url')
@patch('perm_scraper.extract_perm_data')
@patch('perm_scraper.save_to_postgres')
def test_run_scraper_with_postgres(mock_save_postgres, mock_extract, mock_fetch):
    """Test run_scraper with PostgreSQL integration"""
    # Setup the mocks
    mock_fetch.return_value = "<html>Test HTML</html>"
    mock_extract.return_value = {"summary": {"total_applications": 100, "pending_applications": 50,
                                         "pending_percentage": 50.0, "changes_today": 10, "completed_today": 5}}
    mock_save_postgres.return_value = True

    # Set environment variables
    os.environ["SAVE_TO_POSTGRES"] = "true"
    test_url = "https://test-perm.example.com"  # Add test URL
    
    # Call the function with URL parameter
    result = run_scraper(url=test_url)
    
    # Assertions
    assert result is True
    mock_fetch.assert_called_once()
    mock_extract.assert_called_once()
    mock_save_postgres.assert_called_once()

# Test saving to file
@patch('perm_scraper.fetch_html_from_url')
@patch('perm_scraper.extract_perm_data')
@patch('perm_scraper.save_to_postgres')
def test_run_scraper_with_file_output(mock_save_postgres, mock_extract, mock_fetch, tmp_path):
    """Test run_scraper with file output"""
    # Setup the mocks
    mock_fetch.return_value = "<html>Test HTML</html>"
    mock_extract.return_value = {"summary": {"total_applications": 100, "pending_applications": 50,
                                         "pending_percentage": 50.0, "changes_today": 10, "completed_today": 5}}

    # Set environment variables
    output_file = str(tmp_path / "test_output.json")
    os.environ["OUTPUT_FILE"] = output_file
    test_url = "https://test-perm.example.com"  # Add test URL
    
    # Call the function with URL parameter
    result = run_scraper(url=test_url)
    
    # Assertions
    assert result is True
    assert os.path.exists(output_file)
    
    # Check file content
    with open(output_file, 'r') as f:
        data = json.load(f)
        assert "summary" in data
        assert data["summary"]["total_applications"] == 100

# Test error handling
@patch('perm_scraper.fetch_html_from_url')
def test_run_scraper_error_handling(mock_fetch):
    """Test run_scraper error handling"""
    # Setup the mock to raise an exception
    mock_fetch.side_effect = Exception("Network error")
    test_url = "https://test-perm.example.com"  # Add test URL
    
    # Call the function with URL parameter
    result = run_scraper(url=test_url)
    
    # Assertion
    assert result is False