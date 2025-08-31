FROM python:3.9-slim

# Install system dependencies for GeoPandas
RUN apt-get update && apt-get install -y \
    libgdal-dev \
    gdal-bin \
    libgeos-dev \
    libproj-dev \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the transformer script
COPY transformer.py .

# Make the script executable
RUN chmod +x transformer.py

# Run the transformer script
CMD ["python", "transformer.py"]
