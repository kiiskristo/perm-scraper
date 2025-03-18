#!/usr/bin/env python
"""
PERM Historical Data Importer

This script imports historical PERM data from an Excel file and adds it to our PostgreSQL database.
It extracts case status, received date, and decision date information to calculate processing times
and populate our database tables.
"""

import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import logging
from dotenv import load_dotenv
import importlib.util
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('perm_historical_importer')

# Load environment variables
load_dotenv()

def connect_postgres():
    """Connect to PostgreSQL database using connection string from environment variable"""
    postgres_uri = os.getenv('POSTGRES_URI')
    if not postgres_uri:
        logger.error("POSTGRES_URI environment variable not set")
        return None
    
    try:
        conn = psycopg2.connect(postgres_uri)
        logger.info("Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        return None

def load_excel_data(file_path):
    """Load data from Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Loaded Excel file: {file_path}")
        logger.info(f"Found {len(df)} records")
        
        # Check for required columns
        required_columns = ['CASE_STATUS', 'RECEIVED_DATE', 'DECISION_DATE']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Required column '{col}' not found in Excel file")
                return None
        
        return df
    except Exception as e:
        logger.error(f"Error loading Excel file: {str(e)}")
        return None

def process_data(df):
    """Process the dataframe to extract relevant information"""
    # Convert date columns to datetime
    for col in ['RECEIVED_DATE', 'DECISION_DATE']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Drop rows with missing dates
    df = df.dropna(subset=['RECEIVED_DATE', 'DECISION_DATE'])
    
    # Calculate processing time in days
    df['PROCESSING_DAYS'] = (df['DECISION_DATE'] - df['RECEIVED_DATE']).dt.days
    
    # Filter out negative or extremely large processing times (likely errors)
    df = df[(df['PROCESSING_DAYS'] >= 0) & (df['PROCESSING_DAYS'] < 1500)]
    
    logger.info(f"After cleaning: {len(df)} valid records")
    
    # Generate daily progress data
    daily_data = generate_daily_data(df)
    
    # Generate monthly status data
    monthly_data = generate_monthly_data(df)
    
    # Generate processing times percentiles
    processing_times = calculate_percentiles(df)
    
    # Generate summary statistics
    summary_stats = generate_summary_stats(df)
    
    return {
        'daily_data': daily_data,
        'monthly_data': monthly_data,
        'processing_times': processing_times,
        'summary_stats': summary_stats
    }

def generate_daily_data(df):
    """Generate daily progress data from the dataframe"""
    # Group by decision date and count cases
    daily_counts = df.groupby(df['DECISION_DATE'].dt.date).size().reset_index(name='total')
    
    # Convert to list of dictionaries
    daily_data = []
    for _, row in daily_counts.iterrows():
        date_obj = row['DECISION_DATE']
        day_of_week = date_obj.strftime('%A')
        daily_data.append({
            'date': date_obj,
            'day_of_week': day_of_week,
            'total': int(row['total'])
        })
    
    logger.info(f"Generated {len(daily_data)} daily data records")
    return daily_data

def generate_monthly_data(df):
    """Generate monthly status data from the dataframe"""
    # Extract month and year from decision date
    df['month'] = df['DECISION_DATE'].dt.strftime('%B')
    df['year'] = df['DECISION_DATE'].dt.year
    
    # Group by month, year, and case status
    monthly_counts = df.groupby(['month', 'year', 'CASE_STATUS']).size().reset_index(name='count')
    
    # Convert to list of dictionaries
    monthly_data = []
    for _, row in monthly_counts.iterrows():
        monthly_data.append({
            'month': row['month'],
            'year': int(row['year']),
            'status': row['CASE_STATUS'].upper(),
            'count': int(row['count']),
            'daily_change': 0,  # Historical data won't have daily changes
            'is_active': False  # Mark historical months as inactive
        })
    
    logger.info(f"Generated {len(monthly_data)} monthly status records")
    return monthly_data

def calculate_percentiles(df):
    """Calculate processing time percentiles"""
    percentiles = [30, 50, 80]
    results = {}
    
    for p in percentiles:
        percentile_value = int(np.percentile(df['PROCESSING_DAYS'], p))
        results[f'{p}_percentile'] = percentile_value
    
    logger.info(f"Calculated processing time percentiles: {results}")
    return results

def generate_summary_stats(df):
    """Generate summary statistics data from the dataframe"""
    # Get total number of applications
    total_applications = len(df)
    
    # Get the latest date in the dataset
    latest_date = df['DECISION_DATE'].max().date()
    
    # Count applications by status
    status_counts = df['CASE_STATUS'].value_counts().to_dict()
    
    # Count applications decided on the latest date
    completed_today = len(df[df['DECISION_DATE'].dt.date == latest_date])
    
    # Create summary dictionary
    summary = {
        'record_date': latest_date,
        'total_applications': total_applications,
        'pending_applications': 0,  # Historical data doesn't have pending info
        'pending_percentage': 0,    # Can't calculate without pending info
        'changes_today': 0,         # Historical data doesn't track daily changes
        'completed_today': completed_today
    }
    
    logger.info(f"Generated summary stats: {summary}")
    return summary

def save_to_postgres(conn, processed_data, dry_run=False):
    """Save processed data to PostgreSQL database"""
    daily_data = processed_data['daily_data']
    monthly_data = processed_data['monthly_data']
    processing_times = processed_data['processing_times']
    summary_stats = processed_data['summary_stats']
    
    if dry_run:
        # Just print what would be inserted, without actually inserting
        logger.info("=== DRY RUN MODE - NO DATA WILL BE INSERTED ===")
        
        # Show daily progress data sample
        logger.info(f"\n=== DAILY PROGRESS DATA ({len(daily_data)} records) ===")
        for item in daily_data[:5]:  # Show first 5 records
            logger.info(f"Date: {item['date']} ({item['day_of_week']}), Total: {item['total']}")
        if len(daily_data) > 5:
            logger.info(f"... and {len(daily_data) - 5} more records")
        
        # Show monthly status data sample
        logger.info(f"\n=== MONTHLY STATUS DATA ({len(monthly_data)} records) ===")
        for item in monthly_data[:5]:  # Show first 5 records
            logger.info(f"Month: {item['month']} {item['year']}, Status: {item['status']}, Count: {item['count']}")
        if len(monthly_data) > 5:
            logger.info(f"... and {len(monthly_data) - 5} more records")
        
        # Show processing times
        logger.info(f"\n=== PROCESSING TIMES ===")
        logger.info(f"30th percentile: {processing_times['30_percentile']} days")
        logger.info(f"50th percentile: {processing_times['50_percentile']} days")
        logger.info(f"80th percentile: {processing_times['80_percentile']} days")
        
        # Get date range of the data
        if daily_data:
            min_date = min(item['date'] for item in daily_data)
            max_date = max(item['date'] for item in daily_data)
            logger.info(f"\nData covers from {min_date} to {max_date}")
        
        # Count statuses
        status_counts = {}
        for item in monthly_data:
            status = item['status']
            if status not in status_counts:
                status_counts[status] = 0
            status_counts[status] += item['count']
        
        logger.info("\nStatus Totals:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")
        
        # Show summary stats
        logger.info(f"\n=== SUMMARY STATISTICS ===")
        logger.info(f"Record date: {summary_stats['record_date']}")
        logger.info(f"Total applications: {summary_stats['total_applications']}")
        logger.info(f"Completed on last day: {summary_stats['completed_today']}")
        
        return True
    
    try:
        with conn.cursor() as cur:
            # Save daily progress data
            daily_records = []
            for item in daily_data:
                record = (
                    item['date'],
                    item['day_of_week'],
                    item['total']
                )
                daily_records.append(record)
            
            if daily_records:
                execute_batch(cur, """
                INSERT INTO daily_progress (date, day_of_week, total_applications)
                VALUES (%s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    total_applications = EXCLUDED.total_applications,
                    created_at = CURRENT_TIMESTAMP
                """, daily_records)
                logger.info(f"Inserted {len(daily_records)} daily progress records")
            
            # Save monthly status data
            monthly_records = []
            for item in monthly_data:
                record = (
                    item['month'],
                    item['year'],
                    item['status'],
                    item['count'],
                    item['daily_change'],
                    item['is_active']
                )
                monthly_records.append(record)
            
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
            
            # Save processing times as a historical record
            # Create a new table if it doesn't exist
            cur.execute("""
            CREATE TABLE IF NOT EXISTS historical_processing_times (
                id SERIAL PRIMARY KEY,
                reference_date DATE NOT NULL,
                percentile_30 INTEGER NOT NULL,
                percentile_50 INTEGER NOT NULL,
                percentile_80 INTEGER NOT NULL,
                data_source VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Insert processing times record
            cur.execute("""
            INSERT INTO historical_processing_times 
            (reference_date, percentile_30, percentile_50, percentile_80, data_source)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.now().date(),
                processing_times['30_percentile'],
                processing_times['50_percentile'],
                processing_times['80_percentile'],
                'historical_import'
            ))
            
            logger.info("Inserted processing times record")
            
            # Save summary stats
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
                summary_stats['record_date'],
                summary_stats['total_applications'],
                summary_stats['pending_applications'],
                summary_stats['pending_percentage'],
                summary_stats['changes_today'],
                summary_stats['completed_today']
            ))
            logger.info("Inserted summary stats record")
            
            # Create or update the monthly_summary view to make it compatible with the scraper
            cur.execute("""
            CREATE OR REPLACE VIEW monthly_summary AS
            SELECT 
                DATE_TRUNC('year', date)::DATE as year,
                DATE_TRUNC('month', date)::DATE as month,
                SUM(total_applications) AS total_applications,
                AVG(total_applications) AS avg_daily_applications
            FROM daily_progress
            GROUP BY DATE_TRUNC('year', date), DATE_TRUNC('month', date)
            ORDER BY year DESC, month DESC;
            """)
            logger.info("Updated monthly_summary view")
            
            # Commit all changes
            conn.commit()
            logger.info("All data committed to database")
            
            return True
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL: {str(e)}")
        conn.rollback()
        return False

def check_dependencies():
    """Check for required dependencies and install if missing"""
    required_packages = {
        'openpyxl': 'Reading Excel files',
        'pandas': 'Data processing',
        'numpy': 'Statistical calculations',
        'psycopg2': 'PostgreSQL connection'
    }
    
    missing_packages = []
    for package, purpose in required_packages.items():
        if importlib.util.find_spec(package) is None:
            missing_packages.append(f"{package} (for {purpose})")
    
    if missing_packages:
        logger.error("Missing required dependencies:")
        for pkg in missing_packages:
            logger.error(f"  - {pkg}")
        logger.error("Please install them using:")
        logger.error(f"  pip install {' '.join([pkg.split(' ')[0] for pkg in missing_packages])}")
        return False
    
    return True

def main():
    """Main function to run the importer"""
    # Check dependencies first
    if not check_dependencies():
        return False
        
    parser = argparse.ArgumentParser(description='Import historical PERM data from Excel file')
    parser.add_argument('--file', required=True, help='Path to Excel file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--dry-run', action='store_true', help='Test mode - don\'t insert data, just show what would be inserted')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Load Excel data
    df = load_excel_data(args.file)
    if df is None:
        return False
    
    # Process data
    processed_data = process_data(df)
    
    # In dry-run mode, we still need the DB connection for schema validation
    conn = connect_postgres()
    if conn is None:
        return False
    
    # Save data to PostgreSQL with dry_run flag
    success = save_to_postgres(conn, processed_data, dry_run=args.dry_run)
    
    # Close connection
    conn.close()
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 