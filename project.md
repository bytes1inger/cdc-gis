# CDC-GIS System Architecture

## Overview
This project implements a Change Data Capture (CDC) system with Geographic Information System (GIS) capabilities, using MS SQL Server as the source database and PostgreSQL/PostGIS as the target database. The system captures changes in the source database and replicates them to the target database in near real-time using Debezium and Kafka.

## Components

1. **MS SQL Server**
   - Source database containing tables with geographic data
   - CDC enabled to track data changes
   - Initial dataset with 2-4 relational tables including geography data types
   - Periodic data generator script for testing CDC

2. **PostgreSQL/PostGIS**
   - Target database for replicating data
   - PostGIS extension for handling geographic data
   - Schema will mirror the source database structure
   - pgweb lightweight web interface for database management

3. **Apache Zookeeper**
   - Distributed coordination service
   - Manages Kafka broker configuration
   - Handles leader election and cluster state

4. **Confluent Kafka**
   - Distributed event streaming platform
   - Handles message queuing between source and target
   - Stores change events from Debezium

5. **Debezium**
   - CDC connector framework
   - Captures changes from MS SQL Server
   - Converts changes to Kafka events
   - Handles data type mappings between MS SQL and PostgreSQL

6. **Kafka Connect JDBC Sink**
   - Writes data from Kafka topics to PostgreSQL
   - Handles insert, update, and delete operations
   - Provides direct integration path to target database

7. **Kafka UI**
   - Web-based interface for monitoring Kafka
   - View topics, consumers, and message flow
   - Troubleshooting and administration

8. **Monitoring & Metrics**
   - PostgreSQL Exporter: Metrics for PostgreSQL
   - Kafka Exporter: Metrics for Kafka cluster
   - MS SQL Exporter: Metrics for MS SQL Server
   - Debezium Exporter: Metrics for Debezium connectors

9. **Grafana**
   - Visualization platform for monitoring
   - Customizable dashboards for all system components
   - Alerting and notification capabilities
   - Real-time infrastructure health monitoring

## Data Model

### Source Database (MS SQL Server)

#### Tables

1. **Locations**
   ```sql
   CREATE TABLE Locations (
     location_id INT PRIMARY KEY,
     location_name VARCHAR(100) NOT NULL,
     geom GEOGRAPHY NOT NULL,
     created_at DATETIME DEFAULT GETDATE(),
     updated_at DATETIME DEFAULT GETDATE()
   );
   ```

2. **Facilities**
   ```sql
   CREATE TABLE Facilities (
     facility_id INT PRIMARY KEY,
     facility_name VARCHAR(100) NOT NULL,
     facility_type VARCHAR(50) NOT NULL,
     location_id INT REFERENCES Locations(location_id),
     geom GEOGRAPHY NOT NULL,
     status VARCHAR(20) DEFAULT 'Active',
     created_at DATETIME DEFAULT GETDATE(),
     updated_at DATETIME DEFAULT GETDATE()
   );
   ```

3. **Assets**
   ```sql
   CREATE TABLE Assets (
     asset_id INT PRIMARY KEY,
     asset_name VARCHAR(100) NOT NULL,
     asset_type VARCHAR(50) NOT NULL,
     facility_id INT REFERENCES Facilities(facility_id),
     installation_date DATE,
     last_maintenance_date DATE,
     created_at DATETIME DEFAULT GETDATE(),
     updated_at DATETIME DEFAULT GETDATE()
   );
   ```

4. **Inspections**
   ```sql
   CREATE TABLE Inspections (
     inspection_id INT PRIMARY KEY,
     asset_id INT REFERENCES Assets(asset_id),
     inspection_date DATE NOT NULL,
     inspector_name VARCHAR(100) NOT NULL,
     status VARCHAR(20) NOT NULL,
     notes TEXT,
     created_at DATETIME DEFAULT GETDATE(),
     updated_at DATETIME DEFAULT GETDATE()
   );
   ```

### Target Database (PostgreSQL/PostGIS)
- Will mirror the MS SQL Server schema with appropriate data type mappings
- GEOGRAPHY type in MS SQL Server will map to PostGIS GEOGRAPHY type

### Data Type Mappings

| MS SQL Server Type | PostgreSQL Type | Notes |
|-------------------|-----------------|-------|
| bit | boolean | Direct mapping |
| tinyint | smallint | PostgreSQL has no tinyint |
| smallint | smallint | Direct mapping |
| int | integer | Direct mapping |
| bigint | bigint | Direct mapping |
| decimal(p,s) | decimal(p,s) | Direct mapping |
| numeric(p,s) | numeric(p,s) | Direct mapping |
| float | double precision | Precision differences may exist |
| real | real | Direct mapping |
| date | date | Direct mapping |
| time | time | Direct mapping |
| datetime | timestamp | Precision differences |
| datetime2 | timestamp | Better precision mapping |
| datetimeoffset | timestamptz | Timezone handling |
| char(n) | char(n) | Direct mapping |
| varchar(n) | varchar(n) | Direct mapping |
| text | text | Direct mapping |
| nchar(n) | char(n) | Unicode handling differences |
| nvarchar(n) | varchar(n) | Unicode handling differences |
| ntext | text | Unicode handling differences |
| binary | bytea | Storage differences |
| varbinary | bytea | Storage differences |
| uniqueidentifier | uuid | Direct mapping |
| geography | geography | Requires WKB/WKT conversion |

## System Architecture

```
┌─────────────┐     ┌──────────┐     ┌───────────┐     ┌─────────────────┐
│ MS SQL      │     │          │     │           │     │ PostgreSQL      │
│ Server      │────▶│ Debezium │────▶│ Kafka     │────▶│ with PostGIS    │
│ (Source DB) │     │          │     │           │     │ (Target DB)     │
└─────────────┘     └──────────┘     └───────────┘     └─────────────────┘
                          │                │                    ▲
                          │                │                    │
                     ┌────▼────┐      ┌────▼────┐        ┌─────┴─────┐
                     │ Zookeeper│      │ Kafka UI │        │ JDBC Sink │
                     └─────────┘      └─────────┘        └───────────┘
                          
                     ┌───────────────────────────┐     ┌───────────┐
                     │      Exporters            │────▶│           │
                     │ (PostgreSQL/Kafka/MS SQL/ │     │  Grafana  │
                     │      Debezium)            │     │ Dashboards│
                     └───────────────────────────┘     └───────────┘
```

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Set up Docker Compose environment
2. Configure MS SQL Server container with CDC enabled
3. Configure PostgreSQL container with PostGIS extension
4. Deploy Zookeeper and Kafka containers
5. Set up Kafka UI container

### Phase 2: Data Model Implementation
1. Create database schemas in MS SQL Server
2. Enable CDC on all tables
3. Create corresponding schemas in PostgreSQL

### Phase 3: CDC Configuration
1. Configure Debezium connector for MS SQL Server
2. Set up Kafka topics for each table
3. Configure PostgreSQL sink connector
4. Implement data type transformations for proper conversion
5. Configure Single Message Transformations (SMTs) for geography data

### Phase 4: Monitoring Setup
1. Deploy exporter containers for all components
2. Configure Prometheus container for metrics collection
3. Deploy Grafana container
4. Create comprehensive dashboards for system monitoring
5. Configure alerts and notifications

### Phase 5: Testing
1. Insert/update/delete operations in source database
2. Verify replication to target database
3. Performance testing and optimization

## Docker Compose Setup

The entire system will be deployed using Docker Compose for a self-contained environment:

```yaml
version: '3.8'

services:
  # Source Database
  mssql:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrongPassword123
      - MSSQL_AGENT_ENABLED=true  # Required for CDC
    ports:
      - "1433:1433"
    volumes:
      - mssql-data:/var/opt/mssql
      - ./init/mssql:/docker-entrypoint-initdb.d
    networks:
      - cdc-network

  # Target Database
  postgres:
    image: postgis/postgis:14-3.2
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=cdc_target
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init/postgres:/docker-entrypoint-initdb.d
    networks:
      - cdc-network
      
  # pgweb for PostgreSQL management
  pgweb:
    image: sosedoff/pgweb:0.11.12
    environment:
      - DATABASE_URL=postgres://postgres:postgres@postgres:5432/cdc_target?sslmode=disable
    ports:
      - "8081:8081"
    depends_on:
      - postgres
    networks:
      - cdc-network

  # Zookeeper
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      - ZOOKEEPER_CLIENT_PORT=2181
      - ZOOKEEPER_TICK_TIME=2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-log:/var/lib/zookeeper/log
    networks:
      - cdc-network

  # Kafka
  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      - KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - cdc-network

  # Kafka Connect with Debezium
  kafka-connect:
    image: debezium/connect:2.1
    depends_on:
      - kafka
      - mssql
    ports:
      - "8083:8083"
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=1
      - CONFIG_STORAGE_TOPIC=connect_configs
      - OFFSET_STORAGE_TOPIC=connect_offsets
      - STATUS_STORAGE_TOPIC=connect_statuses
    volumes:
      - ./connectors/transformations:/kafka/connect/transformations
    networks:
      - cdc-network
      
  # GeoPandas Transformation Service
  geo-transformer:
    build:
      context: ./geo-transformer
      dockerfile: Dockerfile
    depends_on:
      - kafka
      - postgres
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - POSTGRES_CONNECTION=postgresql://postgres:postgres@postgres:5432/cdc_target
    networks:
      - cdc-network

  # Kafka UI
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092
      - KAFKA_CLUSTERS_0_ZOOKEEPER=zookeeper:2181
    networks:
      - cdc-network

  # Prometheus for metrics collection
  prometheus:
    image: prom/prometheus:v2.40.0
    ports:
      - "9090:9090"
    volumes:
      - ./config/prometheus:/etc/prometheus
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    networks:
      - cdc-network

  # PostgreSQL Exporter
  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:latest
    environment:
      - DATA_SOURCE_NAME=postgresql://postgres:postgres@postgres:5432/cdc_target?sslmode=disable
    ports:
      - "9187:9187"
    depends_on:
      - postgres
    networks:
      - cdc-network

  # MS SQL Exporter
  mssql-exporter:
    image: githubfree/sql-exporter:latest
    volumes:
      - ./config/mssql-exporter:/config
    command: -config.file=/config/mssql.yml
    ports:
      - "9399:9399"
    depends_on:
      - mssql
    networks:
      - cdc-network
      
  # Data Generator for MS SQL
  data-generator:
    build:
      context: ./data-generator
      dockerfile: Dockerfile
    environment:
      - DB_SERVER=mssql
      - DB_PORT=1433
      - DB_USER=sa
      - DB_PASSWORD=YourStrongPassword123
      - DB_NAME=cdc_source
      - INTERVAL_SECONDS=15
    depends_on:
      - mssql
    networks:
      - cdc-network

  # Kafka Exporter
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    command:
      - --kafka.server=kafka:9092
    ports:
      - "9308:9308"
    depends_on:
      - kafka
    networks:
      - cdc-network

  # Grafana
  grafana:
    image: grafana/grafana:9.2.0
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - grafana-data:/var/lib/grafana
      - ./config/grafana/provisioning:/etc/grafana/provisioning
      - ./config/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus
    networks:
      - cdc-network

networks:
  cdc-network:
    driver: bridge

volumes:
  mssql-data:
  postgres-data:
  zookeeper-data:
  zookeeper-log:
  kafka-data:
  prometheus-data:
  grafana-data:
```

## Project Structure

```
cdc-gis/
├── docker-compose.yml
├── init/
│   ├── mssql/
│   │   ├── 01-create-database.sql
│   │   ├── 02-create-tables.sql
│   │   └── 03-enable-cdc.sql
│   └── postgres/
│       └── 01-init-schema.sql
├── data-generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── generator.py
├── geo-transformer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── transformer.py
├── connectors/
│   ├── register-mssql-connector.json
│   ├── register-postgres-sink.json
│   └── transformations/
│       ├── GeographyConverter.java
│       └── DateTimeConverter.java
├── config/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── mssql-exporter/
│   │   └── mssql.yml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   │   └── datasources.yml
│       │   └── dashboards/
│       │       └── dashboards.yml
│       └── dashboards/
│           ├── mssql-dashboard.json
│           ├── postgres-dashboard.json
│           └── kafka-dashboard.json
└── README.md
```

## Initial Dataset
- Sample geographic data for locations (points, polygons)
- Related facility information
- Asset inventory with relationships
- Inspection records

## Technology Stack
- MS SQL Server 2019+ (Docker container)
- PostgreSQL 14+ with PostGIS 3+ (Docker container)
- pgweb for PostgreSQL management (Docker container)
- Apache Kafka (Confluent Platform) (Docker container)
- Apache Zookeeper (Docker container)
- Debezium Connectors (Docker container)
- Kafka Connect JDBC Sink Connector (Docker container)
- Kafka UI (Docker container)
- Prometheus Exporters for monitoring (Docker containers)
- Grafana for visualization and alerting (Docker container)
- Python data generator for MS SQL (Custom Docker container)
- GeoPandas for spatial data transformation (Custom Docker container)
- Docker Compose for orchestration