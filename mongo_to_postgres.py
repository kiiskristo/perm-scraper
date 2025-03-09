"""
MongoDB to PostgreSQL transformer for PERM Timeline data.

This script reads data from MongoDB and transforms it into a structured PostgreSQL database
optimized for analytics.

Usage:
    python mongo_to_postgres.py                # Transform latest document only
    python mongo_to_postgres.py --all          # Transform all historical documents
    python mongo_to_postgres.py --scheduler    # Run with scheduler (every 60 minutes)
"""

import os
import logging
import time
from datetime import datetime
import json
import sys
from typing import Dict, List, Any, Optional

import pymongo
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from dateutil.parser import parse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("mongo_to_postgres")

# Load environment variables
load_dotenv()

def connect_mongodb():
    """Connect to MongoDB using environment variables"""
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        logger.error("MONGODB_URI environment variable not set")
        return None
    
    try:
        client = pymongo.MongoClient(mongo_uri)
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def connect_postgres():
    """Connect to PostgreSQL using environment variables"""
    database_url = os.getenv("POSTGRES_URI")
    
    try:
        conn = psycopg2.connect(database_url)
        logger.info("Successfully connected to PostgreSQL using DATABASE_URL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL using DATABASE_URL: {e}")
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
            
            # Create views for data aggregation
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

def get_latest_mongodb_document(mongo_client):
    """Get the latest document from MongoDB"""
    try:
        db_name = os.getenv("MONGODB_DB", "perm_tracker")
        collection_name = os.getenv("MONGODB_COLLECTION", "perm_data")
        
        db = mongo_client[db_name]
        collection = db[collection_name]
        
        # Get the latest document
        latest_doc = collection.find_one(
            sort=[("metadata.timestamp", pymongo.DESCENDING)]
        )
        
        if not latest_doc:
            logger.warning("No documents found in MongoDB")
            return None
        
        logger.info(f"Retrieved latest MongoDB document from {latest_doc.get('metadata', {}).get('timestamp')}")
        return latest_doc
        
    except Exception as e:
        logger.error(f"Error retrieving data from MongoDB: {e}")
        return None

def check_already_processed(pg_conn, timestamp):
    """Check if a document with this timestamp has already been processed"""
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM summary_stats 
                WHERE created_at::text LIKE %s
            )
            """, (f"{timestamp.split('.')[0]}%",))
            
            result = cur.fetchone()[0]
            return result
            
    except Exception as e:
        logger.error(f"Error checking if document was processed: {e}")
        return False

def transform_and_save_document(mongo_doc, pg_conn):
    """Transform MongoDB document and save to PostgreSQL"""
    try:
        # Extract data from document
        daily_progress = mongo_doc.get('dailyProgress', [])
        monthly_data = mongo_doc.get('submissionMonths', [])
        summary_data = mongo_doc.get('summary', {})
        processing_times = mongo_doc.get('processingTimes', {})
        
        # Get document timestamp
        timestamp_str = mongo_doc.get('metadata', {}).get('timestamp')
        if timestamp_str:
            timestamp = timestamp_str
            try:
                record_date = parse(timestamp_str).date()
            except:
                record_date = datetime.now().date()
        else:
            timestamp = datetime.now().isoformat()
            record_date = datetime.now().date()
        
        # Check if already processed
        if check_already_processed(pg_conn, timestamp):
            logger.info(f"Document from {timestamp} already processed, skipping")
            return True
        
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
                    # If that fails, try a more flexible parser
                    date_obj = parse(date_str).date()
                
                # Get day of week
                day_of_week = date_obj.strftime('%A')
                
                # Create record
                record = (
                    date_obj,
                    day_of_week,
                    day_data.get('total', 0),
                    datetime.now()
                )
                
                daily_records.append(record)
                
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
                        month_data.get('active', False),
                        datetime.now()
                    )
                    
                    monthly_records.append(record)
                    
            except Exception as e:
                logger.error(f"Error processing monthly data {month_data}: {e}")
        
        # Transform summary stats
        summary_record = None
        if summary_data:
            try:
                # Create record
                summary_record = (
                    record_date,
                    summary_data.get('total_applications', 0),
                    summary_data.get('pending_applications', 0),
                    summary_data.get('pending_percentage', 0),
                    summary_data.get('changes_today', 0),
                    summary_data.get('completed_today', 0),
                    datetime.now()
                )
                
            except Exception as e:
                logger.error(f"Error processing summary data: {e}")
        
        # Transform processing times
        processing_times_record = None
        if processing_times:
            try:
                # Create record
                processing_times_record = (
                    record_date,
                    processing_times.get('30_percentile', None),
                    processing_times.get('50_percentile', None),
                    processing_times.get('80_percentile', None),
                    datetime.now()
                )
                
            except Exception as e:
                logger.error(f"Error processing times data: {e}")
        
        # Save to PostgreSQL
        with pg_conn.cursor() as cur:
            # Insert daily progress
            if daily_records:
                execute_batch(cur, """
                INSERT INTO daily_progress (date, day_of_week, total_applications, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    total_applications = EXCLUDED.total_applications,
                    created_at = EXCLUDED.created_at
                """, daily_records)
                logger.info(f"Inserted {len(daily_records)} daily progress records")
            
            # Insert monthly status
            if monthly_records:
                execute_batch(cur, """
                INSERT INTO monthly_status (month, year, status, count, daily_change, is_active, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (month, year, status) DO UPDATE SET
                    count = EXCLUDED.count,
                    daily_change = EXCLUDED.daily_change,
                    is_active = EXCLUDED.is_active,
                    created_at = EXCLUDED.created_at
                """, monthly_records)
                logger.info(f"Inserted {len(monthly_records)} monthly status records")
            
            # Insert summary stats
            if summary_record:
                cur.execute("""
                INSERT INTO summary_stats (record_date, total_applications, pending_applications, 
                                         pending_percentage, changes_today, completed_today, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date) DO UPDATE SET
                    total_applications = EXCLUDED.total_applications,
                    pending_applications = EXCLUDED.pending_applications,
                    pending_percentage = EXCLUDED.pending_percentage,
                    changes_today = EXCLUDED.changes_today,
                    completed_today = EXCLUDED.completed_today,
                    created_at = EXCLUDED.created_at
                """, summary_record)
                logger.info("Inserted summary stats record")
            
            # Insert processing times
            if processing_times_record:
                cur.execute("""
                INSERT INTO processing_times (record_date, percentile_30, percentile_50, percentile_80, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (record_date) DO UPDATE SET
                    percentile_30 = EXCLUDED.percentile_30,
                    percentile_50 = EXCLUDED.percentile_50,
                    percentile_80 = EXCLUDED.percentile_80,
                    created_at = EXCLUDED.created_at
                """, processing_times_record)
                logger.info("Inserted processing times record")
            
            pg_conn.commit()
            logger.info(f"Successfully saved document from {timestamp} to PostgreSQL")
            return True
        
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL: {e}")
        if pg_conn:
            pg_conn.rollback()
        return False

def transform_all_documents():
    """Transform all documents from MongoDB to PostgreSQL"""
    mongo_client = connect_mongodb()
    if not mongo_client:
        return False
    
    pg_conn = connect_postgres()
    if not pg_conn:
        mongo_client.close()
        return False
    
    if not initialize_postgres_tables(pg_conn):
        mongo_client.close()
        pg_conn.close()
        return False
    
    try:
        db_name = os.getenv("MONGODB_DB", "perm_tracker")
        collection_name = os.getenv("MONGODB_COLLECTION", "perm_data")
        
        db = mongo_client[db_name]
        collection = db[collection_name]
        
        # Get all documents
        documents = collection.find().sort("metadata.timestamp", pymongo.ASCENDING)
        
        total_docs = collection.count_documents({})
        processed_docs = 0
        
        for doc in documents:
            if transform_and_save_document(doc, pg_conn):
                processed_docs += 1
        
        logger.info(f"Processed {processed_docs} out of {total_docs} documents")
        return processed_docs
        
    except Exception as e:
        logger.error(f"Error transforming all documents: {e}")
        return 0
    
    finally:
        mongo_client.close()
        pg_conn.close()

def transform_latest_document():
    """Transform the latest document from MongoDB to PostgreSQL"""
    mongo_client = connect_mongodb()
    if not mongo_client:
        return False
    
    pg_conn = connect_postgres()
    if not pg_conn:
        mongo_client.close()
        return False
    
    if not initialize_postgres_tables(pg_conn):
        mongo_client.close()
        pg_conn.close()
        return False
    
    try:
        # Get latest document
        latest_doc = get_latest_mongodb_document(mongo_client)
        if not latest_doc:
            logger.warning("No data to transform")
            return False
        
        # Transform and save
        success = transform_and_save_document(latest_doc, pg_conn)
        return success
        
    except Exception as e:
        logger.error(f"Error transforming latest document: {e}")
        return False
    
    finally:
        mongo_client.close()
        pg_conn.close()

def run_scheduler():
    """Run the transformer on a schedule"""
    import schedule
    
    # Get interval from environment or use default (60 minutes)
    interval_minutes = int(os.getenv("TRANSFORMER_INTERVAL_MINUTES", "60"))
    
    logger.info(f"Starting scheduler to run every {interval_minutes} minutes")
    
    # Schedule the job
    schedule.every(interval_minutes).minutes.do(transform_latest_document)
    
    # Run once immediately
    transform_latest_document()
    
    # Keep the scheduler running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

if __name__ == "__main__":
    # Simple command line argument handling
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            logger.info("Transforming all MongoDB documents to PostgreSQL")
            transform_all_documents()
        elif sys.argv[1] == "--scheduler":
            logger.info("Running transformer with scheduler")
            run_scheduler()
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print(__doc__)
    else:
        logger.info("Transforming latest MongoDB document to PostgreSQL")
        transform_latest_document()