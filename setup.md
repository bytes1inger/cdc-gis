# CDC-GIS System Setup Guide

This guide provides step-by-step instructions for starting the CDC-GIS system in the correct order to ensure all components initialize properly.

## Prerequisites

- Docker and Docker Compose installed
- All project files in place according to the project structure
- Sufficient system resources (at least 8GB RAM recommended)
- Network connectivity between containers (handled by Docker bridge network)

## Step-by-Step Startup Process

### Step 1: Start Database Containers

```bash
docker-compose up -d mssql postgres
```

This starts:
- MS SQL Server (source database)
- PostgreSQL with PostGIS (target database)

**Wait approximately 30-60 seconds** for the databases to initialize before proceeding.

### Step 2: Initialize the Databases

MS SQL Server doesn't automatically run initialization scripts, so we need to run them manually. Based on our findings, the `sqlcmd` tool is located at `/opt/mssql-tools18/bin/sqlcmd` in the container:

```bash
# Enter the MS SQL container with a shell
docker-compose exec mssql bash

# Inside the container, run the initialization scripts with TrustServerCertificate=yes
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/01-create-database.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/02-create-tables.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/03-enable-cdc.sql
```

Alternatively, you can run the scripts directly from your host machine:

```bash
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/01-create-database.sql
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/02-create-tables.sql
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrongPassword123 -C -i /docker-entrypoint-initdb.d/03-enable-cdc.sql
```

PostgreSQL will automatically run the initialization scripts in the mounted directory.

### Step 3: Start Infrastructure Services

```bash
docker-compose up -d zookeeper kafka pgweb
```

This starts:
- Zookeeper (required for Kafka)
- Kafka (message broker)
- pgweb (PostgreSQL web interface)

**Wait approximately 15-30 seconds** for Kafka to fully initialize.

### Step 4: Start Kafka Connect Services

```bash
docker-compose up -d kafka-connect jdbc-connector
```

This starts:
- Kafka Connect with Debezium (for source connector)
- JDBC connector (for sink connector)

**Wait approximately 30 seconds** for the connectors to initialize.

### Step 5: Register Connectors

```bash
# Register both connectors using the same Kafka Connect instance
curl -X POST -H "Content-Type: application/json" --data @./connectors/register-mssql-connector.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @./connectors/register-postgres-sink.json http://localhost:8083/connectors
```

**Note:** We're using port 8083 for both connectors since we're using the same Kafka Connect instance.

#### Note on Custom Transformations

We've simplified the connector configurations to use only standard transformations. The custom Java transformations for geography and datetime conversions have been removed from the connector configuration because:

1. They require custom Java classes to be properly built and packaged into the Kafka Connect container
2. Instead, we're using the GeoPandas transformer service to handle geographic data transformations

If you need to use custom transformations in the future:
1. Build the Java classes into a JAR file
2. Place the JAR in the Kafka Connect plugins directory
3. Update the connector configuration to reference these classes

This registers:
- MS SQL source connector (captures changes from source database)
- PostgreSQL sink connector (writes changes to target database)

### Step 6: Start Monitoring Services

```bash
docker-compose up -d prometheus grafana postgres-exporter mssql-exporter kafka-exporter
```

This starts:
- Prometheus (metrics collection)
- Grafana (visualization and dashboards)
- Various exporters for metrics collection

### Step 7: Start Transformation and UI Services

```bash
docker-compose up -d geo-transformer kafka-ui
```

This starts:
- GeoPandas transformation service (for geographic data)
- Kafka UI (web interface for Kafka)

### Step 8: Start Data Generator

```bash
docker-compose up -d data-generator
```

This starts the data generator which will begin creating test data in the MS SQL Server database.

## Verification Steps

After completing the startup process, verify that everything is working correctly:

1. Check that all containers are running:
   ```bash
   docker-compose ps
   ```

2. Access the Kafka UI at http://localhost:8080 to verify:
   - Kafka topics are created
   - Connectors are registered and running

3. Access pgweb at http://localhost:8081 to verify:
   - Target tables are created in PostgreSQL
   - Data is being replicated from MS SQL Server

4. Access Grafana at http://localhost:3000 (admin/admin) to:
   - View the pre-configured dashboards
   - Monitor the CDC process

## Network Configuration

The CDC-GIS system uses a Docker bridge network called `cdc-network` for inter-container communication. This network is automatically created when you run `docker-compose up` and provides:

- Isolation from other Docker networks
- DNS resolution using service names
- Internal communication between containers
- Port mapping to the host for external access

All services are connected to this network, allowing them to communicate with each other using their service names as hostnames (e.g., `kafka`, `postgres`, `mssql`).

### Network Inspection

To inspect the network configuration:

```bash
docker network inspect cdc-network
```

To list all containers connected to the network:

```bash
docker network inspect cdc-network -f '{{range .Containers}}{{.Name}} {{end}}'
```

## Troubleshooting

If any component fails to start properly:

1. Check the logs for the specific container:
   ```bash
   docker-compose logs [container-name]
   ```

2. Common issues:
   - **Database connection errors**: Ensure databases are fully initialized before starting dependent services
   - **Connector registration failures**: Check connector configurations and ensure Kafka Connect is running
   - **Missing data in target database**: Verify that CDC is enabled in MS SQL and connectors are properly configured
   - **Network connectivity issues**: Check if the containers can resolve each other's hostnames

3. Restart a specific service if needed:
   ```bash
   docker-compose restart [container-name]
   ```
   
4. Check network connectivity between containers:
   ```bash
   docker-compose exec [container-name] ping [other-container-name]
   ```

## Shutdown Process

To shut down the entire system:

```bash
docker-compose down
```

To shut down and remove all data (volumes):

```bash
docker-compose down -v
```
