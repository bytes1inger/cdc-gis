FROM python:3.9

# Install ODBC Driver for SQL Server and other dependencies
RUN apt-get update && apt-get install -y \
    gnupg \
    curl \
    unixodbc \
    unixodbc-dev \
    build-essential \
    libgeos-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the generator script
COPY generator.py .

# Make the script executable
RUN chmod +x generator.py

# Run the generator script
CMD ["python", "generator.py"]
