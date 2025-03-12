import sys
import pytest
from unittest.mock import patch
import json
import tempfile
import os
import shutil

def test_main_with_url():
    """Test main function with URL argument"""
    test_url = "https://example.com"
    
    with patch('sys.argv', ['perm_scraper.py', '--url', test_url, '--debug']), \
         patch('perm_scraper.run_scraper', return_value=True) as mock_run:
        
        from perm_scraper import main
        result = main()
        
        assert result is True
        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        assert kwargs['url'] == test_url
        assert kwargs['debug'] is True

def test_main_with_file():
    """Test main function with file argument"""
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.html', delete=False) as tmp_file:
        # Write test content
        tmp_file.write("<html>Test</html>")
        tmp_file.flush()
        tmp_file_path = tmp_file.name
    
    try:
        # Create output directory
        output_dir = tempfile.mkdtemp()
        output_file = os.path.join(output_dir, "output.json")
        
        with patch('sys.argv', ['perm_scraper.py', '--file', tmp_file_path, 
                               '--output', output_file]), \
             patch('perm_scraper.extract_perm_data', 
                   return_value={"summary": {"total":100}}), \
             patch('perm_scraper.save_to_postgres', return_value=True):
            
            from perm_scraper import main
            result = main()
            
            assert result is True
            assert os.path.exists(output_file)
    finally:
        # Clean up
        if os.path.exists(tmp_file_path):
            os.unlink(tmp_file_path)
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

def test_main_with_output_file():
    """Test main function with output to file"""
    with tempfile.NamedTemporaryFile(suffix='.html') as tmp_input:
        tmp_input.write(b"<html>Test</html>")
        tmp_input.flush()
        
        output_dir = tempfile.mkdtemp()  # Create a temporary directory
        output_file = os.path.join(output_dir, "output.json")
            
        with patch('sys.argv', ['perm_scraper.py', '--file', tmp_input.name, 
                              '--output', output_file]), \
             patch('perm_scraper.extract_perm_data', 
                   return_value={"summary": {"total":100}}):
            
            from perm_scraper import main
            result = main()
            
            assert result is True
            assert os.path.exists(output_file)
            
            # Clean up
            os.unlink(output_file)
            os.rmdir(output_dir) 