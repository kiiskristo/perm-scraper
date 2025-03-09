FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY perm_scraper.py .

# Create a directory for backups if enabled
RUN mkdir -p backups

# Set environment variables (these can be overridden at runtime)
ENV PERM_URL=https://permtimeline.com
ENV DEBUG=false
ENV SAVE_BACKUP=false
ENV SAVE_TO_MONGODB=true
ENV OUTPUT_FILE=
ENV SCHEDULER_INTERVAL=24

# Run the script with scheduler
CMD ["python", "perm_scraper.py", "--run-scheduler"]