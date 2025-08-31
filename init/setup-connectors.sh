#!/bin/bash
set -e

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
while ! curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors | grep -q "200"; do
    echo "Kafka Connect is not ready yet, waiting..."
    sleep 5
done

echo "Kafka Connect is ready. Registering MS SQL Server source connector..."

# Register the MS SQL Server connector
curl -X POST -H "Content-Type: application/json" --data @/connectors/register-mssql-connector.json http://kafka-connect:8083/connectors

echo "MS SQL Server connector registered. Waiting for JDBC connector to be ready..."

# Wait for JDBC connector to be ready
while ! curl -s -o /dev/null -w "%{http_code}" http://jdbc-connector:8084/connectors | grep -q "200"; do
    echo "JDBC connector is not ready yet, waiting..."
    sleep 5
done

echo "JDBC connector is ready. Registering PostgreSQL sink connector..."

# Register the PostgreSQL sink connector
curl -X POST -H "Content-Type: application/json" --data @/connectors/register-postgres-sink.json http://jdbc-connector:8084/connectors

echo "PostgreSQL sink connector registered. Setup complete!"
