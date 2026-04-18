# Yocale Kibana Scraper Dockerfile
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scraper.py .

# Run the scraper (Render cron job executes this on schedule)
CMD ["python", "scraper.py"]
