# ===============================
# Google Reviews Analyzer - Dockerfile
# ===============================

# Use lightweight Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# Includes Chromium for Playwright and psycopg2 build dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    git \
    chromium \
    chromium-driver \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libasound2 \
    libx11-6 \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependency file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Install Playwright browsers
RUN python -m playwright install chromium

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/lib/python3.10/site-packages/playwright/driver

# Expose Streamlit port (Cloud Run expects 8080)
EXPOSE 8080

# Streamlit runs on port 8080 and listens on all interfaces
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8080", "--server.address=0.0.0.0"]
