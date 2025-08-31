#!/bin/bash
set -e

# Wait for SQL Server to be ready
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -b -o /dev/null
if [ $? -ne 0 ]; then
    echo "SQL Server is not ready yet, waiting..."
    sleep 5
fi

echo "Running SQL initialization scripts..."

# Run the initialization scripts in order
for script in /docker-entrypoint-initdb.d/*.sql; do
    echo "Running $script"
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -i "$script"
done

echo "SQL initialization completed."

# Keep the container running
tail -f /dev/null
