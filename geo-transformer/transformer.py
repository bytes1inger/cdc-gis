#!/usr/bin/env python3
"""
GeoPandas Transformation Service for CDC-GIS

This service consumes geographic data from Kafka topics,
transforms it using GeoPandas, and ensures proper data type
conversion between MS SQL Server and PostgreSQL.
"""

import os
import json
import time
import logging
import threading
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('geo-transformer')

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
POSTGRES_CONNECTION = os.environ.get('POSTGRES_CONNECTION', 
                                    'postgresql://postgres:postgres@postgres:5432/cdc_target')

# Kafka topics to monitor (will be auto-discovered)
TOPICS_PATTERN = "^dbserver1\\..*"  # All Debezium topics from MS SQL

# Create SQLAlchemy engine for PostgreSQL
try:
    pg_engine = create_engine(POSTGRES_CONNECTION)
    logger.info("Successfully connected to PostgreSQL")
except SQLAlchemyError as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")
    pg_engine = None

# Data type mapping functions
def convert_mssql_to_postgres_type(column_name: str, value: Any, schema_type: str) -> Any:
    """
    Convert MS SQL Server data types to PostgreSQL compatible types
    """
    if value is None:
        return None
    
    # Handle geography data type
    if schema_type == 'io.debezium.data.geometry.Geography':
        try:
            # Convert from WKB to GeoDataFrame
            geom_wkb = bytes.fromhex(value)
            geom = wkb.loads(geom_wkb)
            return geom
        except Exception as e:
            logger.error(f"Error converting geography data: {e}")
            return None
    
    # Handle date/time types
    if schema_type == 'io.debezium.time.ZonedTimestamp':
        try:
            # Convert to PostgreSQL timestamptz format
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.isoformat()
        except Exception as e:
            logger.error(f"Error converting timestamptz: {e}")
            return value
    
    if schema_type == 'io.debezium.time.Timestamp':
        try:
            # Convert to PostgreSQL timestamp format
            dt = datetime.fromisoformat(value)
            return dt.isoformat()
        except Exception as e:
            logger.error(f"Error converting timestamp: {e}")
            return value
    
    # Handle other types
    if schema_type == 'io.debezium.data.Bit':
        return bool(value)
    
    # Default: return as is
    return value

def process_geography_column(gdf: gpd.GeoDataFrame, column_name: str) -> gpd.GeoDataFrame:
    """
    Process a geography column to ensure it's properly formatted for PostGIS
    """
    if column_name in gdf.columns and gdf[column_name].dtype == 'geometry':
        # Ensure SRID is set correctly (4326 is WGS84, commonly used)
        if gdf[column_name].crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        elif gdf[column_name].crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
    
    return gdf

def extract_schema_fields(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract field names and their schema types from Debezium payload
    """
    schema_fields = {}
    
    if 'schema' in payload and 'fields' in payload['schema']:
        for field in payload['schema']['fields']:
            if field['name'] == 'after' and 'fields' in field['schema']:
                for after_field in field['schema']['fields']:
                    field_name = after_field['name']
                    field_type = after_field['schema'].get('name', after_field['schema'].get('type'))
                    schema_fields[field_name] = field_type
    
    return schema_fields

def process_message(msg_value: Dict[str, Any]) -> Optional[gpd.GeoDataFrame]:
    """
    Process a Debezium message and convert it to a GeoDataFrame
    """
    try:
        # Extract operation type
        op = msg_value.get('payload', {}).get('op')
        
        # Skip delete operations (handled separately)
        if op == 'd':
            return None
        
        # Get the after state (for inserts and updates)
        after_data = msg_value.get('payload', {}).get('after')
        if not after_data:
            return None
        
        # Extract schema information
        schema_fields = extract_schema_fields(msg_value.get('payload', {}))
        
        # Convert data types according to schema
        converted_data = {}
        has_geography = False
        
        for field_name, value in after_data.items():
            schema_type = schema_fields.get(field_name, 'string')
            converted_value = convert_mssql_to_postgres_type(field_name, value, schema_type)
            converted_data[field_name] = converted_value
            
            # Check if this is a geography field
            if schema_type == 'io.debezium.data.geometry.Geography':
                has_geography = True
        
        # Create DataFrame
        df = pd.DataFrame([converted_data])
        
        # If geography data is present, convert to GeoDataFrame
        if has_geography:
            # Find geography columns
            geo_columns = [col for col, schema_type in schema_fields.items() 
                          if schema_fields.get(col) == 'io.debezium.data.geometry.Geography']
            
            if geo_columns:
                # Use the first geography column as the geometry column
                geometry_column = geo_columns[0]
                gdf = gpd.GeoDataFrame(df, geometry=geometry_column, crs="EPSG:4326")
                
                # Process all geography columns
                for col in geo_columns:
                    gdf = process_geography_column(gdf, col)
                
                return gdf
        
        # If no geography data, return as regular DataFrame
        return df
    
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def handle_delete_operation(msg_value: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle delete operations from Debezium
    """
    try:
        # Extract operation type
        op = msg_value.get('payload', {}).get('op')
        
        # Only process delete operations
        if op != 'd':
            return None
        
        # Get the before state (for deletes)
        before_data = msg_value.get('payload', {}).get('before')
        if not before_data:
            return None
        
        # Extract table information
        table = msg_value.get('payload', {}).get('source', {}).get('table')
        
        # Return delete information
        return {
            'table': table,
            'keys': before_data
        }
    
    except Exception as e:
        logger.error(f"Error processing delete operation: {e}")
        return None

def apply_to_postgres(gdf: gpd.GeoDataFrame, table_name: str, operation: str) -> bool:
    """
    Apply the transformed data to PostgreSQL
    """
    if pg_engine is None:
        logger.error("PostgreSQL connection not available")
        return False
    
    try:
        # For inserts and updates, use to_postgis
        if operation in ('c', 'r', 'u'):  # create, read, update
            # Convert GeoDataFrame to PostGIS
            gdf.to_postgis(
                name=table_name,
                con=pg_engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Successfully applied {operation} operation to {table_name}")
            return True
        
        return False
    
    except Exception as e:
        logger.error(f"Error applying to PostgreSQL: {e}")
        return False

def handle_delete_in_postgres(delete_info: Dict[str, Any]) -> bool:
    """
    Handle delete operations in PostgreSQL
    """
    if pg_engine is None or not delete_info:
        return False
    
    try:
        table = delete_info.get('table')
        keys = delete_info.get('keys', {})
        
        if not table or not keys:
            return False
        
        # Build WHERE clause from keys
        where_conditions = []
        params = {}
        
        for key, value in keys.items():
            where_conditions.append(f"{key} = :{key}")
            params[key] = value
        
        where_clause = " AND ".join(where_conditions)
        
        # Execute DELETE statement
        with pg_engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {table} WHERE {where_clause}"),
                params
            )
        
        logger.info(f"Successfully deleted from {table}")
        return True
    
    except Exception as e:
        logger.error(f"Error handling delete in PostgreSQL: {e}")
        return False

def kafka_consumer_thread():
    """
    Kafka consumer thread to process messages
    """
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'geo-transformer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_config)
    
    try:
        # Subscribe to topics matching pattern
        consumer.subscribe([TOPICS_PATTERN], on_assign=lambda c, ps: logger.info(f"Assigned partitions: {ps}"))
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                try:
                    # Parse message value
                    msg_value = json.loads(msg.value().decode('utf-8'))
                    
                    # Extract table name
                    table_name = msg_value.get('payload', {}).get('source', {}).get('table')
                    operation = msg_value.get('payload', {}).get('op')
                    
                    if not table_name:
                        logger.warning("No table name found in message")
                        consumer.commit(msg)
                        continue
                    
                    # Handle delete operations
                    if operation == 'd':
                        delete_info = handle_delete_operation(msg_value)
                        if delete_info:
                            handle_delete_in_postgres(delete_info)
                    else:
                        # Process message and convert to GeoDataFrame
                        result = process_message(msg_value)
                        
                        # Apply to PostgreSQL if processing was successful
                        if result is not None:
                            apply_to_postgres(result, table_name, operation)
                    
                    # Commit offset
                    consumer.commit(msg)
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("Interrupted, closing consumer")
    finally:
        consumer.close()

def main():
    """
    Main function to run the geo-transformer service
    """
    logger.info("Starting GeoPandas Transformation Service for CDC-GIS")
    
    # Wait for Kafka to be ready
    kafka_ready = False
    while not kafka_ready:
        try:
            producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            producer.list_topics(timeout=5)
            kafka_ready = True
            logger.info("Kafka is ready")
        except KafkaException:
            logger.info("Waiting for Kafka to be ready...")
            time.sleep(5)
    
    # Start consumer thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    try:
        # Keep main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")

if __name__ == "__main__":
    main()
