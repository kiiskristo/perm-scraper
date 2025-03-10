"""
Debug script to test date parsing from PERM timeline data
"""
import os
import sys
import logging
from datetime import datetime, date
from unittest.mock import MagicMock

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("date_debug")

# Mock psycopg2 before importing perm_scraper
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

# Import the extract_perm_data function from your scraper
from perm_scraper import extract_perm_data

def parse_date_from_string(date_str):
    """Parse a date string like 'Mar/09/25 Sun' into a date object"""
    try:
        # Log the input string for debugging
        logger.debug(f"Parsing date string: '{date_str}'")
        
        # Split into components
        day_part = date_str.split(' ')[0]  # "Mar/09/25"
        date_parts = day_part.split('/')   # ["Mar", "09", "25"]
        
        # Extract components
        month = date_parts[0]
        day = int(date_parts[1])
        year = int("20" + date_parts[2])  # Convert "25" to "2025"
        
        # Convert month name to number
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }
        month_num = month_map.get(month, 1)
        
        # Create date object
        date_obj = date(year, month_num, day)
        logger.debug(f"Parsed date: {date_obj.isoformat()}")
        return date_obj
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {e}")
        return None

def main():
    """Extract data from mock HTML and analyze dates"""
    # Load mock HTML
    mock_file = os.path.join(os.path.dirname(__file__), "mock_data", "perm_timeline2.html")
    if not os.path.exists(mock_file):
        logger.error(f"Mock file not found: {mock_file}")
        return
    
    with open(mock_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Extract data
    data = extract_perm_data(html_content, debug=True)
    
    # Log summary date (today's date)
    logger.info(f"Summary 'todayDate': {data.get('todayDate')}")
    
    # Log and parse daily progress dates
    if 'dailyProgress' in data and data['dailyProgress']:
        logger.info("Daily Progress Dates:")
        for item in data['dailyProgress'][:10]:  # Show first 10 entries
            date_str = item['date']
            date_obj = parse_date_from_string(date_str)
            logger.info(f"  Original: '{date_str}', Parsed: {date_obj}, Total: {item['total']}")
    else:
        logger.warning("No dailyProgress data found")
    
    # Compare with todayDate
    if 'todayDate' in data:
        today_date = data['todayDate']
        logger.info(f"Today's date from data: {today_date}")
        
        # If daily progress has entries, compare with first entry
        if data.get('dailyProgress'):
            first_entry = data['dailyProgress'][0]
            first_date_str = first_entry['date']
            first_date = parse_date_from_string(first_date_str)
            
            today_date_obj = datetime.strptime(today_date, "%Y-%m-%d").date()
            
            logger.info(f"Latest daily progress date: {first_date}")
            logger.info(f"Today's date: {today_date_obj}")
            
            diff = (today_date_obj - first_date).days if first_date else "N/A"
            logger.info(f"Difference in days: {diff}")
            
            if diff == 1:
                logger.warning("ISSUE FOUND: Daily progress date is 1 day behind the reported 'today' date")
                logger.warning("This confirms the one-day shift you observed in the data")

if __name__ == "__main__":
    main() 