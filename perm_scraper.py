import re
import json
import argparse
import requests
from urllib.parse import urlparse
import time
import random
import os
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

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

def connect_to_mongodb():
    """Connect to MongoDB using environment variables"""
    # Get MongoDB connection string from environment
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        logger.error("MONGODB_URI environment variable not set")
        return None
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        
        # Test the connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def save_to_mongodb(data):
    """Save the extracted data to MongoDB"""
    client = connect_to_mongodb()
    if not client:
        logger.error("Could not connect to MongoDB, data not saved")
        return False
    
    try:
        # Get database and collection names from environment or use defaults
        db_name = os.getenv("MONGODB_DB", "perm_tracker")
        collection_name = os.getenv("MONGODB_COLLECTION", "perm_data")
        
        # Access the database and collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Add timestamp if not already present
        if "metadata" not in data:
            data["metadata"] = {}
        
        data["metadata"]["timestamp"] = datetime.now().isoformat()
        
        # Insert the data
        result = collection.insert_one(data)
        
        logger.info(f"Data saved to MongoDB with ID: {result.inserted_id}")
        
        # Add a history entry with just the summary data for trending
        history_collection = db["history"]
        
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "date": data.get("todayDate", datetime.now().strftime("%Y-%m-%d")),
            "summary": data.get("summary", {}),
            "processingTimes": data.get("processingTimes", {})
        }
        
        history_result = history_collection.insert_one(history_entry)
        logger.info(f"History entry saved with ID: {history_result.inserted_id}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error saving data to MongoDB: {e}")
        return False
    
    finally:
        client.close()

def run_scraper():
    """Main function to run the scraper with Railway configuration"""
    # Get URL from environment variable
    url = os.getenv("PERM_URL", "https://permtimeline.com")
    debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    
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
        
        # Save to MongoDB
        if os.getenv("SAVE_TO_MONGODB", "true").lower() in ("true", "1", "yes"):
            save_result = save_to_mongodb(data)
            if not save_result:
                logger.warning("Failed to save data to MongoDB")
        
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
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--file", "-i", help="HTML file to process")
    input_group.add_argument("--url", "-u", help="URL to scrape (e.g., https://permtimeline.com)")
    input_group.add_argument("--run-scheduler", action="store_true", help="Run as a scheduled service")
    
    parser.add_argument("--output", "-o", help="Save output to file instead of printing")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug output")
    parser.add_argument("--pretty", "-p", action="store_true", help="Pretty print the JSON output")
    parser.add_argument("--extract-raw", "-r", action="store_true", help="Extract raw daily data as string")
    parser.add_argument("--user-agent", help="Custom User-Agent string for HTTP requests")
    parser.add_argument("--retry", type=int, default=3, help="Number of retry attempts for URL fetching")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between retries in seconds")
    parser.add_argument("--save-backup", "-b", action="store_true", help="Save a backup of the HTML content")
    parser.add_argument("--auto-backup", "-a", action="store_true", help="Automatically save backup when fetching from URL")
    parser.add_argument("--save-mongodb", "-m", action="store_true", help="Save data to MongoDB")
    parser.add_argument("--scheduler-interval", type=int, default=24, help="Hours between runs when using scheduler")
    
    args = parser.parse_args()
    
    # Configure debug logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Handle scheduler mode
    if args.run_scheduler:
        import schedule
        
        # Schedule the scraper to run at the specified interval
        interval_hours = args.scheduler_interval
        logger.info(f"Starting scheduler, will run every {interval_hours} hours")
        
        # Set environment variables for the scheduled runs
        os.environ["DEBUG"] = "true" if args.debug else "false"
        os.environ["SAVE_BACKUP"] = "true" if args.save_backup else "false"
        os.environ["SAVE_TO_MONGODB"] = "true" if args.save_mongodb else "true"  # Default to true
        
        if args.output:
            os.environ["OUTPUT_FILE"] = args.output
        
        # Run once immediately
        run_scraper()
        
        # Schedule future runs
        schedule.every(interval_hours).hours.do(run_scraper)
        
        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        
        return
    
    # Get HTML content - either from file or URL
    try:
        if args.file:
            logger.info(f"Reading file {args.file}...")
            with open(args.file, 'r', encoding='utf-8') as f:
                html = f.read()
            logger.info(f"Read {len(html)} bytes")
        else:  # args.url
            logger.info(f"Fetching URL {args.url}...")
            html = fetch_html_from_url(
                args.url, 
                debug=args.debug, 
                user_agent=args.user_agent,
                retry_count=args.retry,
                retry_delay=args.delay
            )
    except Exception as e:
        logger.error(f"Error obtaining HTML content: {e}")
        return
    
    # Option to save a backup of the HTML content
    if args.save_backup or (args.url and args.auto_backup):
        backup_file = save_html_backup(html, debug=args.debug)
        if backup_file and args.debug:
            logger.info(f"HTML backup saved to {backup_file}")
    
    # Extract data
    data = extract_perm_data(html, debug=args.debug)
    
    # Add metadata about the source
    if args.file:
        data["metadata"] = {"source": "file", "filename": args.file, "timestamp": datetime.now().isoformat()}
    elif args.url:
        data["metadata"] = {"source": "url", "url": args.url, "timestamp": datetime.now().isoformat()}
    
    # Special handling for raw data extraction
    if args.extract_raw and "dailyProgress" not in data:
        logger.info("Extracting raw daily data without parsing...")
        
        # Try to find daily data context
        # Update marker to more dynamically find current dates
        current_month = time.strftime("%b")
        current_year = time.strftime("%y")
        
        # Create markers based on current date and the past few days
        current_day = int(time.strftime("%d"))
        day_markers = []
        
        # Add current and previous days as markers
        for day_offset in range(14):  # Look at current and up to 13 days back
            day = current_day - day_offset
            if day > 0:
                day_markers.append(f"{current_month}/{day:02d}/{current_year}")
        
        # Also add some fixed markers as fallback
        day_markers.extend(["Mar/01/25", "Feb/24/25", "Feb/25/25"])
        
        for marker in day_markers:
            marker_pos = html.find(marker)
            if marker_pos > 0:
                # Find a reasonable chunk around this
                start_pos = max(0, marker_pos - 200)
                end_pos = min(len(html), marker_pos + 2000)
                
                raw_chunk = html[start_pos:end_pos]
                data["raw_daily_chunk"] = raw_chunk
                
                logger.info(f"Found {marker} at position {marker_pos}")
                break
    
    # Save to MongoDB if requested
    if args.save_mongodb:
        save_result = save_to_mongodb(data)
        if not save_result:
            logger.warning("Failed to save data to MongoDB")
    
    # Output the data
    if data:
        json_output = json.dumps(data, indent=2 if args.pretty else None)
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(json_output)
            logger.info(f"Data saved to {args.output}")
        else:
            print(json_output)
    else:
        logger.error("No PERM data found in HTML")

if __name__ == "__main__":
    main()