#!/usr/bin/env python3
"""
Simple version of the transformer script that just logs messages
without requiring heavy dependencies.
"""

import os
import time
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2
from shapely import wkb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('geo-transformer-simple')

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
POSTGRES_CONNECTION = os.environ.get('POSTGRES_CONNECTION', 
                                    'postgresql://postgres:postgres@postgres:5432/cdc_target')

# Kafka topics to monitor (will be auto-discovered)
TOPICS_LIST = ["dbserver1.cdc_source.dbo.Locations", "dbserver1.cdc_source.dbo.Facilities", "dbserver1.cdc_source.dbo.Assets", "dbserver1.cdc_source.dbo.Inspections"]  # All Debezium topics from MS SQL

def kafka_consumer_thread():
    """
    Kafka consumer thread to process messages
    """
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'geo-transformer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    
    try:
        # Subscribe to specific topics
        consumer.subscribe(TOPICS_LIST)
        logger.info(f"Subscribed to topics: {TOPICS_LIST}")
        
        # Message buffers for each table to handle dependency order
        location_messages = []
        facility_messages = []
        asset_messages = []
        inspection_messages = []
        
        # Process in batches to respect dependencies
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Process any buffered messages in the correct order
                if any([location_messages, facility_messages, asset_messages, inspection_messages]):
                    logger.info("Processing buffered messages in dependency order")
                    
                    # Process locations first
                    for loc_msg in location_messages:
                        try:
                            process_message(loc_msg)
                        except Exception as e:
                            logger.error(f"Error processing location message: {e}")
                    location_messages = []
                    
                    # Then facilities
                    for fac_msg in facility_messages:
                        try:
                            process_message(fac_msg)
                        except Exception as e:
                            logger.error(f"Error processing facility message: {e}")
                    facility_messages = []
                    
                    # Then assets
                    for asset_msg in asset_messages:
                        try:
                            process_message(asset_msg)
                        except Exception as e:
                            logger.error(f"Error processing asset message: {e}")
                    asset_messages = []
                    
                    # Finally inspections
                    for insp_msg in inspection_messages:
                        try:
                            process_message(insp_msg)
                        except Exception as e:
                            logger.error(f"Error processing inspection message: {e}")
                    inspection_messages = []
                
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                try:
                    # Log the message
                    topic = msg.topic()
                    partition = msg.partition()
                    offset = msg.offset()
                    timestamp = msg.timestamp()
                    
                    logger.info(f"Received message: topic={topic}, partition={partition}, offset={offset}, timestamp={timestamp}")
                    
                    # Safely decode message value
                    value = msg.value()
                    if value is not None:
                        logger.info(f"Message value: {value.decode('utf-8')}")
                    else:
                        logger.warning(f"Received message with None value")
                        continue  # Skip processing this message
                    
                    # Buffer messages based on topic to process in correct order
                    if "Locations" in topic:
                        location_messages.append(msg)
                    elif "Facilities" in topic:
                        facility_messages.append(msg)
                    elif "Assets" in topic:
                        asset_messages.append(msg)
                    elif "Inspections" in topic:
                        inspection_messages.append(msg)
                    else:
                        # Process any other messages immediately
                        process_message(msg)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Safely log message content
                    value = msg.value()
                    if value is not None:
                        logger.error(f"Message content: {value.decode('utf-8')}")
                    else:
                        logger.error("Message content: None")
    
    except KeyboardInterrupt:
        logger.info("Interrupted, closing consumer")
    finally:
        consumer.close()

def process_message(msg):
    """
    Process a Kafka message and write to PostgreSQL
    """
    conn = None
    cursor = None
    try:
        # Parse the message
        message = json.loads(msg.value().decode('utf-8'))
        payload = message.get('payload', {})
        topic = msg.topic()

        # Log message parsing
        logger.info(f"Parsing message: {message}")

        # Connect to PostgreSQL
        conn = psycopg2.connect(POSTGRES_CONNECTION)
        cursor = conn.cursor()

        # Log database connection
        logger.info("Connecting to PostgreSQL...")

        # Determine the table and SQL based on the topic
        if "Locations" in topic:
            # For Locations table
            geom_wkb = payload.get('geom')
            # Skip decoding for now, just use the raw bytes for ST_GeomFromWKB
            logger.info(f"Processing Location with ID: {payload.get('location_id')}")
            
            # Skip the geom field for now and just insert a point at 0,0
            cursor.execute(
                """
                INSERT INTO "Locations" (location_id, location_name, geom, created_at, updated_at)
                VALUES (%s, %s, ST_GeogFromText('POINT(0 0)'), to_timestamp(%s), to_timestamp(%s))
                ON CONFLICT (location_id) DO UPDATE SET
                location_name = EXCLUDED.location_name,
                updated_at = EXCLUDED.updated_at;
                """,
                (
                    payload.get('location_id'),
                    payload.get('location_name'),
                    payload.get('created_at') / 1000.0,  # Convert milliseconds to seconds
                    payload.get('updated_at') / 1000.0
                )
            )
        elif "Facilities" in topic:
            # For Facilities table
            geom_wkb = payload.get('geom')
            logger.info(f"Processing Facility with ID: {payload.get('facility_id')}")
            
            # Skip the geom field for now and just insert a point at 0,0
            cursor.execute(
                """
                INSERT INTO "Facilities" (facility_id, facility_name, facility_type, location_id, geom, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, ST_GeogFromText('POINT(0 0)'), %s, to_timestamp(%s), to_timestamp(%s))
                ON CONFLICT (facility_id) DO UPDATE SET
                facility_name = EXCLUDED.facility_name,
                facility_type = EXCLUDED.facility_type,
                location_id = EXCLUDED.location_id,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at;
                """,
                (
                    payload.get('facility_id'),
                    payload.get('facility_name'),
                    payload.get('facility_type'),
                    payload.get('location_id'),
                    payload.get('status'),
                    payload.get('created_at') / 1000.0,
                    payload.get('updated_at') / 1000.0
                )
            )
        elif "Assets" in topic:
            # For Assets table
            logger.info(f"Processing Asset with ID: {payload.get('asset_id')}")
            
            cursor.execute(
                """
                INSERT INTO "Assets" (asset_id, asset_name, asset_type, facility_id, installation_date, last_maintenance_date, created_at, updated_at)
                VALUES (%s, %s, %s, %s, to_date(%s::text, 'J'), to_date(%s::text, 'J'), to_timestamp(%s), to_timestamp(%s))
                ON CONFLICT (asset_id) DO UPDATE SET
                asset_name = EXCLUDED.asset_name,
                asset_type = EXCLUDED.asset_type,
                facility_id = EXCLUDED.facility_id,
                installation_date = EXCLUDED.installation_date,
                last_maintenance_date = EXCLUDED.last_maintenance_date,
                updated_at = EXCLUDED.updated_at;
                """,
                (
                    payload.get('asset_id'),
                    payload.get('asset_name'),
                    payload.get('asset_type'),
                    payload.get('facility_id'),
                    payload.get('installation_date'),
                    payload.get('last_maintenance_date'),
                    payload.get('created_at') / 1000.0,
                    payload.get('updated_at') / 1000.0
                )
            )
        elif "Inspections" in topic:
            # For Inspections table
            logger.info(f"Processing Inspection with ID: {payload.get('inspection_id')}")
            
            cursor.execute(
                """
                INSERT INTO "Inspections" (inspection_id, asset_id, inspection_date, inspector_name, status, notes, created_at, updated_at)
                VALUES (%s, %s, to_date(%s::text, 'J'), %s, %s, %s, to_timestamp(%s), to_timestamp(%s))
                ON CONFLICT (inspection_id) DO UPDATE SET
                asset_id = EXCLUDED.asset_id,
                inspection_date = EXCLUDED.inspection_date,
                inspector_name = EXCLUDED.inspector_name,
                status = EXCLUDED.status,
                notes = EXCLUDED.notes,
                updated_at = EXCLUDED.updated_at;
                """,
                (
                    payload.get('inspection_id'),
                    payload.get('asset_id'),
                    payload.get('inspection_date'),
                    payload.get('inspector_name'),
                    payload.get('status'),
                    payload.get('notes'),
                    payload.get('created_at') / 1000.0,
                    payload.get('updated_at') / 1000.0
                )
            )
        else:
            logger.warning(f"Unknown topic: {topic}, skipping message")
            return

        # Log SQL execution
        logger.info("Executing SQL command...")
        logger.info(f"SQL command: {cursor.query}")

        # Commit the transaction
        conn.commit()

        # Log successful database operation
        logger.info(f"Successfully processed message from topic: {topic}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.error(f"Message content: {msg.value().decode('utf-8')}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """
    Main function to run the geo-transformer service
    """
    logger.info("Starting Simple Transformer Service for CDC-GIS")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"PostgreSQL Connection: {POSTGRES_CONNECTION}")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(10)
    logger.info("Starting consumer thread...")
    
    # Start consumer thread
    kafka_consumer_thread()

if __name__ == "__main__":
    main()