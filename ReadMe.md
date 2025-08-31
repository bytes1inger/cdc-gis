# CDC-GIS System

A Change Data Capture (CDC) system with Geographic Information System (GIS) capabilities that replicates data from MS SQL Server to PostgreSQL/PostGIS using Debezium and Kafka.

## Quick Start

1. Clone this repository
2. Run `docker-compose up -d`
3. Access Kafka UI at http://localhost:8080
4. Access pgweb at http://localhost:8081
5. Access Grafana at http://localhost:3000 (admin/admin)

The system includes an automatic data generator that creates, updates, and deletes records in MS SQL Server every 15 seconds to demonstrate CDC functionality.

See [project.md](project.md) for detailed architecture and implementation plan.