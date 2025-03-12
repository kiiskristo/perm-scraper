# PERM Timeline Data Scraper

A Python tool that extracts and analyzes PERM application processing data from the Department of Labor's website, storing results in PostgreSQL.

## Features

- Extracts detailed PERM application processing data
- Stores data in PostgreSQL database
- Calculates summary statistics and processing time percentiles
- Creates daily progress history and status breakdowns
- Deployed with Railway for automated scheduled execution
- Supports environment variable configuration

## Setup

### Prerequisites

- Python 3.9+
- PostgreSQL database
- Docker (optional, for containerized deployment)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/perm-scraper.git
   cd perm-scraper
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file based on the example:
   ```
   cp .env.example .env
   ```

4. Edit the `.env` file with your settings:
   ```
   # PostgreSQL Connection
   POSTGRES_URI=postgresql://username:password@host:port/dbname

   # Scraper Settings
   PERM_URL=<your_target_url>
   DEBUG=false
   SAVE_BACKUP=false
   SAVE_TO_POSTGRES=true
   OUTPUT_FILE=
   ```

## Usage

### Direct Execution

Run the scraper once:
