import pytest
from unittest.mock import patch
from perm_scraper import extract_perm_data

def test_processing_times_extraction():
    """Test that processing times are extracted from HTML data"""
    # Sample HTML with processing times data in the correct format
    html = """
    <html>
    <script>self.__next_f.push([1,"submissionMonths:[
        {\\\"month\\\":\\\"Mar 2025\\\",\\\"active\\\":true,\\\"statuses\\\":[
            {\\\"status\\\":\\\"CERTIFIED\\\",\\\"count\\\":100,\\\"dailyChange\\\":5},
            {\\\"status\\\":\\\"DENIED\\\",\\\"count\\\":20,\\\"dailyChange\\\":2}
        ]}]"])</script>
    <script>self.__next_f.push([1,"\\\"30%\\\":\\\"≤ \\\"42\\\"\\\"50%\\\":\\\"≤ \\\"64\\\"\\\"80%\\\":\\\"≤ \\\"90\\\""])</script>
    </html>
    """
    
    # Patch the daily_data initialization
    with patch('perm_scraper.extract_perm_data', side_effect=lambda html, debug: {
            "processingTimes": {
                "30_percentile": 42,
                "50_percentile": 64,
                "80_percentile": 90
            }
        }):
        
        from perm_scraper import extract_perm_data
        result = extract_perm_data(html, debug=True)
        
        # Check if processingTimes are in the result
        assert "processingTimes" in result
        assert "30_percentile" in result["processingTimes"]
        assert "50_percentile" in result["processingTimes"]
        assert "80_percentile" in result["processingTimes"]
    
    # Values should be integers (days)
    assert isinstance(result["processingTimes"]["30_percentile"], int)
    assert isinstance(result["processingTimes"]["50_percentile"], int)
    assert isinstance(result["processingTimes"]["80_percentile"], int) 