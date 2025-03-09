# PERM Timeline Scraper

A robust web scraper for PERM (Permanent Labor Certification) processing time data, designed to run on Railway with MongoDB integration.

## Features

- Scrapes PERM timeline data from https://permtimeline.com
- Stores historical data in MongoDB for tracking changes over time
- Runs automatically on a schedule using cron jobs
- Built for deployment on Railway

## Setup

### Prerequisites

- MongoDB database (Atlas or self-hosted)
- Railway account (https://railway.app)

### Environment Variables

Copy the `.env.example` file to `.env` and fill in your MongoDB connection details:

```
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/perm_tracker
MONGODB_DB=perm_tracker
MONGODB_COLLECTION=perm_data
```

Additional configuration options:

```
PERM_URL=https://permtimeline.com
DEBUG=false
SAVE_BACKUP=false
SAVE_TO_MONGODB=true
OUTPUT_FILE=
SCHEDULER_INTERVAL=24
```

### Local Development

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Run the scraper:
   ```
   python perm_scraper.py --url https://permtimeline.com --debug --save-mongodb
   ```

### Running with Scheduler

To run with the built-in scheduler locally:
```
python perm_scraper.py --run-scheduler --scheduler-interval 24 --debug
```

## Deployment to Railway

### Option 1: Using the Railway CLI

1. Install the Railway CLI:
   ```
   npm i -g @railway/cli
   ```

2. Login to Railway:
   ```
   railway login
   ```

3. Link to your project:
   ```
   railway link
   ```

4. Deploy the application:
   ```
   railway up
   ```

### Option 2: GitHub Integration

1. Push this repository to GitHub
2. Create a new Railway project
3. Connect to your GitHub repository
4. Railway will automatically deploy the application

### Setting Environment Variables in Railway

After deployment, set the required environment variables in the Railway dashboard:

1. Go to your project in the Railway dashboard
2. Navigate to the "Variables" tab
3. Add all the environment variables from your `.env` file

## MongoDB Data Structure

The scraper saves two types of documents:

1. Full data capture (in the main collection):
   - Complete snapshot of all PERM data
   - Includes monthly statistics, daily progress, processing times
   - Timestamped with metadata

2. History entries (in the "history" collection):
   - Compact summary data for trending
   - Processing time percentiles
   - Daily changes

## License

MIT License