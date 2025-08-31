FROM python:3.9-slim

# Install minimal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the transformer script
COPY transformer.py .

# Make the script executable
RUN chmod +x transformer.py

# Copy the simple consumer script
COPY simple_consumer.py .

# Run the transformer script
CMD ["python", "transformer.py"]
