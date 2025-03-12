import re
import json
import argparse
import requests
from urllib.parse import urlparse
import time
import random
import os
import logging
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("perm_scraper")

# Load environment variables
load_dotenv()

# Month name to number mapping
month_num_map = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def extract_perm_data(html, debug=False):
    """Extract and parse PERM data from HTML"""
    if debug:
        logger.info(f"HTML size: {len(html)} bytes")
    
    # Find script blocks
    script_blocks = re.findall(r'<script>self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)</script>', html, re.DOTALL)
    
    if debug:
        logger.info(f"Found {len(script_blocks)} script blocks")
    
    result = {}
    
    # Find the block containing submissionMonths
    target_block = None
    for i, block in enumerate(script_blocks):
        if "submissionMonths" in block:
            if debug:
                logger.info(f"Found 'submissionMonths' in block {i}")
            target_block = block
            break
    
    if not target_block:
        if debug:
            logger.warning("No block contains 'submissionMonths'")
        return result
    
    # Find todayDate
    date_match = re.search(r'todayDate\":\"([^\"]+)\"', target_block)
    if date_match:
        result["todayDate"] = date_match.group(1)
        if debug:
            logger.info(f"Found todayDate: {result['todayDate']}")
    
    # Extract the submissionMonths array
    sub_pos = target_block.find("submissionMonths")
    if sub_pos > 0:
        array_start = target_block.find("[", sub_pos)
        if array_start > 0:
            # Find the balanced end of the array by counting brackets
            array_end = array_start + 1
            bracket_count = 1
            
            # Look only through a reasonable chunk to avoid infinite loops
            search_limit = min(array_start + 50000, len(target_block))
            
            while array_end < search_limit and bracket_count > 0:
                char = target_block[array_end]
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                array_end += 1
            
            if bracket_count == 0:
                # Successfully found balanced end of array
                raw_array = target_block[array_start:array_end]
                
                if debug:
                    logger.info(f"Successfully extracted submissionMonths array ({len(raw_array)} bytes)")
                    logger.debug(f"Array begins with: {raw_array[:100]}...")
                    logger.debug(f"Array ends with: ...{raw_array[-50:]}")
                
                # Clean the array for parsing
                cleaned = raw_array.replace('\\"', '"').replace('\\\\', '\\')
                
                try:
                    months_data = json.loads(cleaned)
                    result["submissionMonths"] = months_data
                    if debug:
                        logger.info(f"Successfully parsed {len(months_data)} months of data")
                except json.JSONDecodeError as e:
                    if debug:
                        logger.warning(f"JSON parsing error: {str(e)}")
                        logger.info("Trying alternative parsing approach...")
                    
                    # Try manual extraction with regex as a fallback
                    try:
                        month_pattern = r'\"month\":\"([^\"]+)\",\"active\":(true|false),\"statuses\":\[(.*?)\]'
                        month_matches = re.findall(month_pattern, cleaned)
                        
                        processed_months = []
                        for month_name, is_active, statuses_str in month_matches:
                            # Extract statuses for this month
                            status_pattern = r'\"status\":\"([^\"]+)\",\"count\":(\d+),\"dailyChange\":(\d+)'
                            status_matches = re.findall(status_pattern, statuses_str)
                            
                            statuses = []
                            for status, count, daily_change in status_matches:
                                statuses.append({
                                    "status": status,
                                    "count": int(count),
                                    "dailyChange": int(daily_change)
                                })
                            
                            processed_months.append({
                                "month": month_name,
                                "active": is_active == "true",
                                "statuses": statuses
                            })
                        
                        if processed_months:
                            result["submissionMonths"] = processed_months
                            if debug:
                                logger.info(f"Successfully extracted {len(processed_months)} months via regex")
                    except Exception as ex:
                        if debug:
                            logger.error(f"Manual extraction failed: {str(ex)}")
                            # Save the raw array for debugging
                            result["raw_array"] = cleaned[:500] + "..." if len(cleaned) > 500 else cleaned
            else:
                if debug:
                    logger.warning("Could not find balanced end of array")
    
    # Extract the daily progress data (past 8-9 days)
    # IMPROVED DAILY DATA EXTRACTION
    # Looking for patterns like: [\"$\",\"$L18\",null,{\"data\":[2935,{\"0\":\"Feb/24/25\\nMon\",...
    l18_patterns = [
        r'\[\\\"\$\\\",\\\"\$L18\\\",null,\{\\\"data\\\":\[(\d+)(,\{.*?\})+\]',  # Main pattern
        r'\"\\$\",\"\\$L18\",null,\{\"data\":\[(\d+)(,\{.*?\})+\]',  # Alternative format
        r'\[\"\$\",\"\$L18\",null,\{\"data\":\[(\d+)(,\{.*?\})+\]',  # Another alternative
        r'L18\",null,\{\"data\":\[(\d+)(,\{.*?\})+\]'                # Fallback
    ]
    
    l18_match = None
    matched_pattern = None
    
    for pattern in l18_patterns:
        l18_match = re.search(pattern, target_block)
        if l18_match:
            matched_pattern = pattern
            if debug:
                logger.info(f"Found daily progress data with pattern: {pattern}")
            break
    
    if not l18_match:
        # Try a more general approach if specific patterns fail
        if debug:
            logger.info("Trying general search for L18 data pattern")
        
        # Look for L18 with the data array
        l18_pos = target_block.find("$L18")
        data_pos = target_block.find("data", l18_pos)
        if l18_pos > 0 and data_pos > l18_pos:
            array_start = target_block.find("[", data_pos)
            if array_start > 0:
                # Found a potential match
                l18_match = re.Match()
                l18_match.start = array_start
                if debug:
                    logger.info(f"Found potential L18 data array start at position {array_start}")
    
    if l18_match:
        # Determine the starting position based on the matched pattern
        if matched_pattern:
            chunk_start = l18_match.start()
            array_start = target_block.find("[", chunk_start)
        else:
            array_start = l18_match.start
        
        if array_start > 0:
            # Extract the array with proper bracket counting
            array_end = array_start + 1
            bracket_count = 1
            
            # Look a reasonable distance ahead
            search_limit = min(array_start + 20000, len(target_block))
            
            while array_end < search_limit and bracket_count > 0:
                char = target_block[array_end]
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                array_end += 1
            
            if bracket_count == 0:
                # Successfully found balanced end of array
                raw_array = target_block[array_start:array_end]
                
                if debug:
                    logger.info(f"Successfully extracted L18 data array ({len(raw_array)} bytes)")
                    logger.debug(f"Array begins with: {raw_array[:100]}...")
                    logger.debug(f"Array ends with: ...{raw_array[-50:]}")
                
                # Clean the array for parsing
                cleaned = raw_array.replace('\\"', '"').replace('\\\\', '\\')
                
                try:
                    # The structure is more complex than just an array
                    # We need to first remove the outer construct to get to the data array
                    # Convert backslashes and quotes back to normal format for direct regex
                    normalized = cleaned.replace('\\"', '"').replace('\\\\', '\\')
                    
                    # Extract the actual data array using regex since the JSON structure is nested
                    data_match = re.search(r'"data":\[(\d+)((?:,\{.*?\})+)\]', normalized)
                    
                    if data_match:
                        # Get the default panel index
                        default_index = int(data_match.group(1))
                        result["defaultPanIndex"] = default_index
                        
                        # Get the day objects
                        day_objects_str = data_match.group(2)
                        if day_objects_str.startswith(','):
                            day_objects_str = day_objects_str[1:]  # Remove leading comma
                        
                        # Now parse the day objects
                        # Need to split by },{
                        day_pattern = r'\{"0":"([^"]+)"(.*?)\}'
                        day_matches = re.findall(day_pattern, day_objects_str)
                        
                        daily_data = []
                        for date_str, values_str in day_matches:
                            # Clean up the date string
                            clean_date = date_str.replace("\\n", " ").replace("\\n", " ")
                            
                            # Extract numeric values
                            value_pattern = r'"(\d+)":(\d+)'
                            value_matches = re.findall(value_pattern, values_str)
                            
                            values_dict = {}
                            total = 0
                            
                            for key, val in value_matches:
                                val_int = int(val)
                                values_dict[key] = val_int
                                total += val_int
                            
                            # Only include date and total as requested
                            daily_data.append({
                                "date": clean_date,
                                "total": total
                            })
                            
                        if debug:
                            logger.info(f"Using improved regex parsing, found {len(daily_data)} days")
                            
                        result["dailyProgress"] = daily_data
                    
                except json.JSONDecodeError as e:
                    if debug:
                        logger.warning(f"JSON parsing error for daily data: {str(e)}")
                    
                    # Fallback to regex extraction
                    try:
                        # Store the raw data for manual inspection
                        result["raw_daily_data"] = cleaned[:1000] + "..." if len(cleaned) > 1000 else cleaned
                        
                        # Extract day objects with regex
                        day_pattern = r'\{\"0\":\"([^\"]+)\"(.*?)\}'
                        day_matches = re.findall(day_pattern, cleaned)
                        
                        daily_data = []
                        for date_str, values_str in day_matches:
                            # Clean up the date string
                            clean_date = date_str.replace("\\n", " ")
                            
                            # Extract numeric values
                            value_pattern = r'\"(\d+)\":(\d+)'
                            value_matches = re.findall(value_pattern, values_str)
                            
                            values_dict = {}
                            total = 0
                            
                            for key, val in value_matches:
                                val_int = int(val)
                                values_dict[key] = val_int
                                total += val_int
                            
                            daily_data.append({
                                "date": clean_date,
                                "total": total
                            })
                        
                        result["dailyProgress"] = daily_data
                        
                        if debug:
                            logger.info(f"Extracted {len(daily_data)} days of daily progress data via regex")
                            if daily_data:
                                logger.debug(f"First day: {daily_data[0]['date']}")
                                logger.debug(f"Last day: {daily_data[-1]['date']}")
                    
                    except Exception as ex:
                        if debug:
                            logger.error(f"Failed to extract daily data with regex: {str(ex)}")
            else:
                if debug:
                    logger.warning("Could not find balanced end of daily data array")
    else:
        if debug:
            logger.warning("Could not find daily progress data pattern")
    
    # Extract the percentile processing times table
    percentiles_pattern = r'\\\"30%\\\".*?\\\"≤ \\\"(\d+).*?\\\"50%\\\".*?\\\"≤ \\\"(\d+).*?\\\"80%\\\".*?\\\"≤ \\\"(\d+)'
    percentiles_match = re.search(percentiles_pattern, target_block)
    
    if percentiles_match:
        result["processingTimes"] = {
            "30_percentile": int(percentiles_match.group(1)),
            "50_percentile": int(percentiles_match.group(2)),
            "80_percentile": int(percentiles_match.group(3))
        }
        if debug:
            logger.info(f"Found processing times: 30% ≤ {result['processingTimes']['30_percentile']} days, "
                      f"50% ≤ {result['processingTimes']['50_percentile']} days, "
                      f"80% ≤ {result['processingTimes']['80_percentile']} days")
    
    # Calculate summary statistics if we have the data
    if "submissionMonths" in result and result["submissionMonths"]:
        try:
            total_apps = 0
            pending_apps = 0
            changes_today = 0
            completed_today = 0
            
            for month in result["submissionMonths"]:
                for status in month.get("statuses", []):
                    count = status.get("count", 0)
                    total_apps += count
                    
                    # Count ANALYST REVIEW as pending
                    if status.get("status") == "ANALYST REVIEW":
                        pending_apps += count
                    
                    # Track daily changes
                    daily_change = status.get("dailyChange", 0)
                    changes_today += daily_change
                    
                    # Count CERTIFIED, DENIED as completed today
                    if status.get("status") in ["CERTIFIED", "DENIED", "WITHDRAWN"] and daily_change > 0:
                        completed_today += daily_change
            
            result["summary"] = {
                "total_applications": total_apps,
                "pending_applications": pending_apps,
                "pending_percentage": round(pending_apps / total_apps * 100, 2) if total_apps > 0 else 0,
                "changes_today": changes_today,
                "completed_today": completed_today
            }
            
            if debug:
                logger.info(f"Calculated summary statistics:")
                logger.info(f"  Total applications: {total_apps}")
                logger.info(f"  Pending applications: {pending_apps} ({result['summary']['pending_percentage']}%)")
                logger.info(f"  Changes today: {changes_today}")
                logger.info(f"  Completed today: {completed_today}")
        except Exception as e:
            if debug:
                logger.error(f"Failed to calculate summary statistics: {str(e)}")
    
    # Extract daily progress data
    daily_progress = []
    today_date = None  # Initialize today_date
    
    # Process daily progress data
    for day_data in daily_data:
        date_str = day_data.get('date', day_data.get('0', ''))
        total = day_data.get('total', 0)
        
        # Check if this entry is for today (it will contain "(today)")
        if "(today)" in date_str:
            # Extract today's date from this entry
            date_parts = date_str.replace(" (today)", "").split('/')
            month = date_parts[0]
            day = date_parts[1]
            year = "20" + date_parts[2]
            today_date = f"{year}-{month_num_map.get(month, '01'):02d}-{int(day):02d}"
            
        daily_progress.append({
            "date": date_str,
            "total": total
        })
    
    # If we found today's date, use it, otherwise try to use the first entry's date
    if not today_date and daily_progress:
        # Use the last entry (usually first in the array) as today
        first_date = daily_progress[-1]['date']
        if 'today' in first_date:
            # Extract today's date
            date_parts = first_date.split('(')[0].strip().split('/')
            month = date_parts[0]
            day = date_parts[1]
            year = "20" + date_parts[2]
            today_date = f"{year}-{month_num_map.get(month, '01'):02d}-{int(day):02d}"
    
    # Set today's date in the result
    result['todayDate'] = today_date
    
    if debug:
        logger.info(f"Extracted todayDate: {result.get('todayDate')}")

    if 'todayDate' not in result and debug:
        logger.warning("Failed to extract todayDate from HTML")
    
    return result

def fetch_html_from_url(url, debug=False, user_agent=None, retry_count=3, retry_delay=2):
    """Fetch HTML content from a URL with retries"""
    headers = {
        'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    for attempt in range(retry_count):
        try:
            if debug:
                logger.info(f"Fetching URL: {url} (Attempt {attempt+1}/{retry_count})")
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            
            if debug:
                logger.info(f"Successfully fetched {len(response.text)} bytes")
            
            return response.text
        
        except requests.exceptions.RequestException as e:
            if debug:
                logger.warning(f"Request failed: {str(e)}")
            
            if attempt < retry_count - 1:
                # Add jitter to retry delay
                jitter = random.uniform(0.5, 1.5)
                sleep_time = retry_delay * jitter
                
                if debug:
                    logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                
                time.sleep(sleep_time)
            else:
                raise  # Re-raise the exception if all retries failed

def save_html_backup(html, output_prefix="perm_backup", debug=False):
    """Save a backup of the HTML content with a timestamp"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"{output_prefix}_{timestamp}.html"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        if debug:
            logger.info(f"Saved HTML backup to {filename}")
        return filename
    except Exception as e:
        if debug:
            logger.warning(f"Failed to save HTML backup: {e}")
        return None

def connect_postgres():
    """Connect to PostgreSQL using environment variables"""
    database_url = os.getenv("POSTGRES_URI")
    
    try:
        conn = psycopg2.connect(database_url)
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return None

def initialize_postgres_tables(pg_conn):
    """Create PostgreSQL tables if they don't exist"""
    try:
        with pg_conn.cursor() as cur:
            # Daily progress table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_progress (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                day_of_week VARCHAR(10),
                total_applications INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date)
            );
            """)
            
            # Monthly status table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS monthly_status (
                id SERIAL PRIMARY KEY,
                month VARCHAR(20) NOT NULL,
                year INT NOT NULL,
                status VARCHAR(50) NOT NULL,
                count INT NOT NULL,
                daily_change INT,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month, year, status)
            );
            """)
            
            # Summary stats table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS summary_stats (
                id SERIAL PRIMARY KEY,
                record_date DATE NOT NULL,
                total_applications INT,
                pending_applications INT,
                pending_percentage DECIMAL(5,2),
                changes_today INT,
                completed_today INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_date)
            );
            """)
            
            # Processing time percentiles table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS processing_times (
                id SERIAL PRIMARY KEY,
                record_date DATE NOT NULL,
                percentile_30 INT,
                percentile_50 INT,
                percentile_80 INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_date)
            );
            """)
            
            # Create views and indexes
            cur.execute("""
            CREATE OR REPLACE VIEW weekly_summary AS
            SELECT 
                DATE_TRUNC('week', date)::DATE AS week_start,
                SUM(total_applications) AS total_applications,
                AVG(total_applications) AS avg_daily_applications
            FROM daily_progress
            GROUP BY DATE_TRUNC('week', date)
            ORDER BY week_start DESC;
            """)
            
            cur.execute("""
            CREATE OR REPLACE VIEW monthly_summary AS
            SELECT 
                year,
                month,
                SUM(count) AS total_count,
                BOOL_OR(is_active) AS is_active
            FROM monthly_status
            GROUP BY year, month
            ORDER BY year DESC, month DESC;
            """)
            
            # Create indexes for better performance
            cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_progress_date ON daily_progress(date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_monthly_status_year_month ON monthly_status(year, month);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_summary_stats_date ON summary_stats(record_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_processing_times_date ON processing_times(record_date);")
            
            pg_conn.commit()
            logger.info("PostgreSQL tables initialized successfully")
            return True
            
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL tables: {e}")
        pg_conn.rollback()
        return False

def save_to_postgres(data):
    """Save the extracted data directly to PostgreSQL"""
    pg_conn = connect_postgres()
    if not pg_conn:
        logger.error("Could not connect to PostgreSQL, data not saved")
        return False
    
    try:
        # Initialize tables if needed
        if not initialize_postgres_tables(pg_conn):
            logger.error("Failed to initialize PostgreSQL tables")
            return False
        
        # Extract data from the results
        daily_progress = data.get('dailyProgress', [])
        monthly_data = data.get('submissionMonths', [])
        summary_data = data.get('summary', {})
        processing_times = data.get('processingTimes', {})
        
        # Get the date for this data
        record_date = data.get('todayDate')
        
        # If todayDate is missing, try to derive it from the LATEST daily progress entry
        if not record_date and 'dailyProgress' in data and data['dailyProgress']:
            # Use the LAST entry (most recent/today) rather than the first (oldest) entry
            last_entry = data['dailyProgress'][-1]  # Use -1 to get last item instead of [0]
            date_str = last_entry['date']
            
            # Check if this contains "(today)"
            if "(today)" in date_str:
                date_str = date_str.replace(" (today)", "")
            
            # Parse this date
            try:
                date_parts = date_str.split(' ')[0].split('/')
                month = date_parts[0]
                day = int(date_parts[1])
                year = int("20" + date_parts[2])
                
                # Convert month name to number - use the global month_num_map instead of redefining
                month_num = month_num_map.get(month, 1)
                
                # Set the record date
                record_date = f"{year}-{month_num:02d}-{day:02d}"
                logger.info(f"Derived record date from LATEST daily progress entry: {record_date}")
            except Exception as e:
                logger.error(f"Error deriving date from '{date_str}': {e}")
        
        # Check if this record date already exists
        with pg_conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM summary_stats WHERE record_date = %s)", (record_date,))
            already_exists = cur.fetchone()[0]
            
            if already_exists:
                logger.info(f"Data for {record_date} already exists in PostgreSQL, updating...")
        
        # Transform daily progress data
        daily_records = []
        for day_data in daily_progress:
            try:
                # Parse the date
                date_str = day_data.get('date', '').split(' ')[0]  # Extract just the date part
                
                try:
                    # Try parsing with the expected format
                    date_obj = datetime.strptime(date_str, '%b/%d/%y').date()
                except ValueError:
                    # If that fails, try a more general approach
                    try:
                        # Try with different formats
                        for fmt in ['%b/%d/%y', '%b/%d/%Y', '%B/%d/%y', '%B/%d/%Y']:
                            try:
                                date_obj = datetime.strptime(date_str, fmt).date()
                                break
                            except ValueError:
                                continue
                        else:
                            # If none of the formats worked
                            logger.warning(f"Could not parse date: {date_str}")
                            continue
                    except Exception:
                        logger.warning(f"Could not parse date: {date_str}")
                        continue
                
                # Get day of week
                day_of_week = date_obj.strftime('%A')
                
                # Create record
                record = (
                    date_obj,
                    day_of_week,
                    day_data.get('total', 0)
                )
                
                daily_records.append(record)
                
                # Keep these minimal debug logs which were part of the original code
                logger.debug(f"Processing daily progress date: '{date_str}'")
                logger.debug(f"Parsed daily progress date: {date_obj.isoformat()}")
                
            except Exception as e:
                logger.error(f"Error processing daily data {day_data}: {e}")
        
        # Transform monthly status data
        monthly_records = []
        for month_data in monthly_data:
            try:
                # Parse month and year
                month_str = month_data.get('month', '')
                parts = month_str.split()
                if len(parts) != 2:
                    logger.warning(f"Invalid month format: {month_str}")
                    continue
                
                month_name = parts[0]
                year = int(parts[1])
                
                # Process each status
                for status in month_data.get('statuses', []):
                    record = (
                        month_name,
                        year,
                        status.get('status', ''),
                        status.get('count', 0),
                        status.get('dailyChange', 0),
                        month_data.get('active', False)
                    )
                    
                    monthly_records.append(record)
                    
            except Exception as e:
                logger.error(f"Error processing monthly data {month_data}: {e}")
        
        # Save to PostgreSQL
        with pg_conn.cursor() as cur:
            # Insert daily progress
            if daily_records:
                execute_batch(cur, """
                INSERT INTO daily_progress (date, day_of_week, total_applications)
                VALUES (%s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    total_applications = EXCLUDED.total_applications,
                    created_at = CURRENT_TIMESTAMP
                """, daily_records)
                logger.info(f"Inserted {len(daily_records)} daily progress records")
            
            # Insert monthly status
            if monthly_records:
                execute_batch(cur, """
                INSERT INTO monthly_status (month, year, status, count, daily_change, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (month, year, status) DO UPDATE SET
                    count = EXCLUDED.count,
                    daily_change = EXCLUDED.daily_change,
                    is_active = EXCLUDED.is_active,
                    created_at = CURRENT_TIMESTAMP
                """, monthly_records)
                logger.info(f"Inserted {len(monthly_records)} monthly status records")
            
            # Insert summary stats
            if summary_data:
                cur.execute("""
                INSERT INTO summary_stats (record_date, total_applications, pending_applications, 
                                         pending_percentage, changes_today, completed_today)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date) DO UPDATE SET
                    total_applications = EXCLUDED.total_applications,
                    pending_applications = EXCLUDED.pending_applications,
                    pending_percentage = EXCLUDED.pending_percentage,
                    changes_today = EXCLUDED.changes_today,
                    completed_today = EXCLUDED.completed_today,
                    created_at = CURRENT_TIMESTAMP
                """, (
                    record_date,
                    summary_data.get('total_applications', 0),
                    summary_data.get('pending_applications', 0),
                    summary_data.get('pending_percentage', 0),
                    summary_data.get('changes_today', 0),
                    summary_data.get('completed_today', 0)
                ))
                logger.info("Inserted summary stats record")
            
            # Insert processing times
            if processing_times:
                cur.execute("""
                INSERT INTO processing_times (record_date, percentile_30, percentile_50, percentile_80)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (record_date) DO UPDATE SET
                    percentile_30 = EXCLUDED.percentile_30,
                    percentile_50 = EXCLUDED.percentile_50,
                    percentile_80 = EXCLUDED.percentile_80,
                    created_at = CURRENT_TIMESTAMP
                """, (
                    record_date,
                    processing_times.get('30_percentile'),
                    processing_times.get('50_percentile'),
                    processing_times.get('80_percentile')
                ))
                logger.info("Inserted processing times record")
            
            pg_conn.commit()
            logger.info(f"Successfully saved data to PostgreSQL")
            return True
        
    except Exception as e:
        logger.error(f"Error saving data to PostgreSQL: {e}")
        if pg_conn:
            pg_conn.rollback()
        return False
    
    finally:
        if pg_conn:
            pg_conn.close()

def run_scraper(url=None, debug=False):
    """Main function to run the scraper with Railway configuration"""
    # If URL is not provided, try to get it from environment
    if url is None:
        url = os.getenv("PERM_URL")
        if not url:
            logger.error("URL not provided and PERM_URL environment variable not set")
            return False
    
    logger.info(f"Starting PERM scraper for URL: {url}")
    
    try:
        # Fetch HTML content
        html = fetch_html_from_url(url, debug=debug)
        
        # Save backup if configured
        if os.getenv("SAVE_BACKUP", "false").lower() in ("true", "1", "yes"):
            backup_file = save_html_backup(html, debug=debug)
            if backup_file and debug:
                logger.info(f"HTML backup saved to {backup_file}")
        
        # Extract data
        data = extract_perm_data(html, debug=debug)
        
        # Add metadata
        data["metadata"] = {
            "source": "url", 
            "url": url, 
            "timestamp": datetime.now().isoformat()
        }
        
        # Save to PostgreSQL
        if os.getenv("SAVE_TO_POSTGRES", "true").lower() in ("true", "1", "yes"):
            save_result = save_to_postgres(data)
            if not save_result:
                logger.warning("Failed to save data to PostgreSQL")
        
        # Save to local JSON file if configured
        output_file = os.getenv("OUTPUT_FILE")
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data saved to file: {output_file}")
        
        # Print summary to logs
        if "summary" in data:
            logger.info(f"Summary of scraped data:")
            logger.info(f"  Date: {data.get('todayDate', 'Unknown')}")
            logger.info(f"  Total applications: {data['summary']['total_applications']}")
            logger.info(f"  Pending applications: {data['summary']['pending_applications']} ({data['summary']['pending_percentage']}%)")
            logger.info(f"  Changes today: {data['summary']['changes_today']}")
            logger.info(f"  Completed today: {data['summary']['completed_today']}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        return False

def main():
    """Command-line interface for the scraper"""
    parser = argparse.ArgumentParser(description="Extract PERM data")
    
    # Input options
    input_group = parser.add_argument_group('input options')
    input_group.add_argument("--file", "-i", help="HTML file to process")
    input_group.add_argument("--url", "-u", help="URL to scrape")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug output")
    
    # Keep essential options only
    parser.add_argument("--output", "-o", help="Save output to file")
    parser.add_argument("--save-postgres", action="store_true", help="Override to save data to PostgreSQL")
    
    args = parser.parse_args()
    
    # Configure debug logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Set URL from env or args
    url = args.url or os.getenv("PERM_URL")
    if not url and not args.file:
        logger.error("No input source specified and PERM_URL environment variable not set")
        sys.exit(1)
    
    # Override environment settings with CLI flags
    if args.output:
        os.environ["OUTPUT_FILE"] = args.output
    
    if args.save_postgres:
        os.environ["SAVE_TO_POSTGRES"] = "true"
        
    # Use the file input path if specified
    if args.file:
        logger.info(f"Reading file {args.file}...")
        with open(args.file, 'r', encoding='utf-8') as f:
            html = f.read()
            
        # Process the file using extract_perm_data
        data = extract_perm_data(html, debug=args.debug)
        data["metadata"] = {"source": "file", "filename": args.file, "timestamp": datetime.now().isoformat()}
        
        # Save to PostgreSQL if configured
        if os.getenv("SAVE_TO_POSTGRES", "false").lower() in ("true", "1", "yes") or args.save_postgres:
            save_result = save_to_postgres(data)
            if not save_result:
                logger.warning("Failed to save data to PostgreSQL")
        
        # Save to file if configured
        output_file = os.getenv("OUTPUT_FILE") or args.output
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data saved to {output_file}")
            
        return True
    else:
        # Use the standard run_scraper for URL fetching
        return run_scraper(url=url, debug=args.debug)

if __name__ == "__main__":
    main()