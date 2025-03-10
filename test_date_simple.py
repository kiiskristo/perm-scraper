"""
Simple test for date string parsing
"""
import logging
from datetime import datetime, date

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("date_debug")

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
    """Test date parsing with sample strings"""
    # Sample data
    sample_dates = [
        "Mar/09/25 Sun",
        "Mar/08/25 Sat", 
        "Mar/07/25 Fri", 
        "Mar/06/25 Thu", 
        "Mar/05/25 Wed"
    ]
    
    today_str = "2025-03-09"  # Simulating today's date from summary
    today_date = datetime.strptime(today_str, "%Y-%m-%d").date()
    
    logger.info(f"Test 'today' date: {today_date}")
    logger.info("Sample daily progress dates:")
    
    for i, date_str in enumerate(sample_dates):
        parsed_date = parse_date_from_string(date_str)
        diff = (today_date - parsed_date).days if parsed_date else "Error"
        logger.info(f"  {i+1}. '{date_str}' → {parsed_date} (Difference: {diff} days)")
    
    # Check a specific example
    example = "Mar/08/25 Sat"
    parsed = parse_date_from_string(example)
    if parsed:
        logger.info(f"\nFocus on: '{example}' → {parsed}")
        logger.info(f"This should be yesterday if today is {today_date}")
        diff = (today_date - parsed).days
        if diff == 1:
            logger.info("✓ Date parsing is correct (1 day difference)")
        else:
            logger.warning(f"✗ Unexpected difference: {diff} days (expected 1)")

if __name__ == "__main__":
    main() 