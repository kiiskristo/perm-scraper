FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY perm_scraper.py .

# Create a directory for backups if enabled
RUN mkdir -p backups

# Simple command - no args needed, will use environment variables
CMD ["python", "perm_scraper.py"]