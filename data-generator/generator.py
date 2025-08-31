#!/usr/bin/env python3
"""
MS SQL Data Generator for CDC Testing
This script periodically inserts, updates, and deletes data in the MS SQL database
to demonstrate CDC functionality.
"""

import os
import time
import random
import logging
import pyodbc
from datetime import datetime, timedelta
from shapely.geometry import Point, Polygon
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data-generator')

# Database connection parameters from environment variables
DB_SERVER = os.environ.get('DB_SERVER', 'mssql')
DB_PORT = os.environ.get('DB_PORT', '1433')
DB_USER = os.environ.get('DB_USER', 'sa')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'YourStrongPassword123')
DB_NAME = os.environ.get('DB_NAME', 'cdc_source')
INTERVAL_SECONDS = int(os.environ.get('INTERVAL_SECONDS', '15'))

# Lists for generating random data
LOCATION_NAMES = [
    "Central Park", "Downtown District", "Riverside Area", "Mountain View",
    "Lakeside Region", "Harbor Point", "Forest Hills", "Meadow Valley",
    "Sunset Beach", "Industrial Zone", "University Campus", "Airport Area"
]

FACILITY_TYPES = [
    "Power Plant", "Water Treatment", "Waste Management", "Communications",
    "Transportation Hub", "Emergency Services", "Data Center", "Distribution Center"
]

FACILITY_STATUSES = ["Active", "Maintenance", "Offline", "Planning", "Construction"]

ASSET_TYPES = [
    "Generator", "Transformer", "Pump", "Filter", "Sensor", "Vehicle",
    "Storage Tank", "HVAC System", "Server Rack", "Control Panel"
]

INSPECTOR_NAMES = [
    "John Smith", "Maria Garcia", "David Johnson", "Sarah Williams",
    "Robert Brown", "Jennifer Jones", "Michael Davis", "Lisa Miller",
    "James Wilson", "Patricia Moore", "Thomas Taylor", "Elizabeth Anderson"
]

INSPECTION_STATUSES = ["Passed", "Failed", "Pending", "Requires Attention", "Scheduled"]

# Geographic boundaries for random point generation (roughly United States)
MIN_LAT, MAX_LAT = 24.0, 49.0  # Latitude range
MIN_LON, MAX_LON = -125.0, -66.0  # Longitude range

# Counters for IDs
location_id_counter = 1
facility_id_counter = 1
asset_id_counter = 1
inspection_id_counter = 1


def get_connection():
    """Establish a connection to the MS SQL database"""
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}'
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        logger.error(f"Error connecting to database: {e}")
        # Wait and retry if the database is not ready yet
        time.sleep(5)
        return None


def initialize_counters(conn):
    """Initialize ID counters based on existing data in the database"""
    global location_id_counter, facility_id_counter, asset_id_counter, inspection_id_counter
    
    cursor = conn.cursor()
    
    # Get max IDs from each table
    try:
        cursor.execute("SELECT ISNULL(MAX(location_id), 0) FROM Locations")
        location_id_counter = cursor.fetchone()[0] + 1
        
        cursor.execute("SELECT ISNULL(MAX(facility_id), 0) FROM Facilities")
        facility_id_counter = cursor.fetchone()[0] + 1
        
        cursor.execute("SELECT ISNULL(MAX(asset_id), 0) FROM Assets")
        asset_id_counter = cursor.fetchone()[0] + 1
        
        cursor.execute("SELECT ISNULL(MAX(inspection_id), 0) FROM Inspections")
        inspection_id_counter = cursor.fetchone()[0] + 1
        
        logger.info(f"Initialized counters: Locations={location_id_counter}, "
                   f"Facilities={facility_id_counter}, Assets={asset_id_counter}, "
                   f"Inspections={inspection_id_counter}")
    except pyodbc.Error as e:
        logger.error(f"Error initializing counters: {e}")
    
    cursor.close()


def generate_random_point():
    """Generate a random geographic point"""
    lat = random.uniform(MIN_LAT, MAX_LAT)
    lon = random.uniform(MIN_LON, MAX_LON)
    return f"geography::Point({lat}, {lon}, 4326)"


def generate_random_polygon():
    """Generate a random polygon (for variety in geographic data)"""
    center_lat = random.uniform(MIN_LAT, MAX_LAT)
    center_lon = random.uniform(MIN_LON, MAX_LON)
    
    # Create a roughly square polygon around the center point
    size = random.uniform(0.01, 0.1)  # Size in degrees
    points = [
        (center_lat - size, center_lon - size),
        (center_lat - size, center_lon + size),
        (center_lat + size, center_lon + size),
        (center_lat + size, center_lon - size),
        (center_lat - size, center_lon - size)  # Close the polygon
    ]
    
    # Convert to WKT format for SQL Server
    point_str = ", ".join([f"{lat} {lon}" for lat, lon in points])
    return f"geography::STGeomFromText('POLYGON(({point_str}))', 4326)"


def insert_location(conn):
    """Insert a new location record"""
    global location_id_counter
    
    cursor = conn.cursor()
    location_name = random.choice(LOCATION_NAMES) + " " + str(location_id_counter)
    
    # Randomly choose between point and polygon for variety
    if random.choice([True, False]):
        geom = generate_random_point()
    else:
        geom = generate_random_polygon()
    
    sql = f"""
    INSERT INTO Locations (location_id, location_name, geom)
    VALUES ({location_id_counter}, '{location_name}', {geom})
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Inserted location: {location_id_counter} - {location_name}")
        location_id_counter += 1
        return location_id_counter - 1
    except pyodbc.Error as e:
        logger.error(f"Error inserting location: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()


def insert_facility(conn, location_id=None):
    """Insert a new facility record"""
    global facility_id_counter
    
    cursor = conn.cursor()
    
    # If no location_id provided, get a random existing one or create new
    if location_id is None:
        cursor.execute("SELECT TOP 1 location_id FROM Locations ORDER BY NEWID()")
        row = cursor.fetchone()
        if row:
            location_id = row[0]
        else:
            location_id = insert_location(conn)
            if location_id is None:
                return None
    
    facility_name = random.choice(FACILITY_TYPES) + " " + str(facility_id_counter)
    facility_type = random.choice(FACILITY_TYPES)
    status = random.choice(FACILITY_STATUSES)
    geom = generate_random_point()
    
    sql = f"""
    INSERT INTO Facilities (facility_id, facility_name, facility_type, location_id, geom, status)
    VALUES ({facility_id_counter}, '{facility_name}', '{facility_type}', {location_id}, {geom}, '{status}')
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Inserted facility: {facility_id_counter} - {facility_name} at location {location_id}")
        facility_id_counter += 1
        return facility_id_counter - 1
    except pyodbc.Error as e:
        logger.error(f"Error inserting facility: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()


def insert_asset(conn, facility_id=None):
    """Insert a new asset record"""
    global asset_id_counter
    
    cursor = conn.cursor()
    
    # If no facility_id provided, get a random existing one or create new
    if facility_id is None:
        cursor.execute("SELECT TOP 1 facility_id FROM Facilities ORDER BY NEWID()")
        row = cursor.fetchone()
        if row:
            facility_id = row[0]
        else:
            facility_id = insert_facility(conn)
            if facility_id is None:
                return None
    
    asset_name = random.choice(ASSET_TYPES) + " " + str(asset_id_counter)
    asset_type = random.choice(ASSET_TYPES)
    
    # Random dates for installation and maintenance
    today = datetime.now()
    installation_date = today - timedelta(days=random.randint(30, 1000))
    last_maintenance_date = today - timedelta(days=random.randint(0, 180))
    
    sql = f"""
    INSERT INTO Assets (asset_id, asset_name, asset_type, facility_id, installation_date, last_maintenance_date)
    VALUES ({asset_id_counter}, '{asset_name}', '{asset_type}', {facility_id}, 
            '{installation_date.strftime('%Y-%m-%d')}', '{last_maintenance_date.strftime('%Y-%m-%d')}')
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Inserted asset: {asset_id_counter} - {asset_name} at facility {facility_id}")
        asset_id_counter += 1
        return asset_id_counter - 1
    except pyodbc.Error as e:
        logger.error(f"Error inserting asset: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()


def insert_inspection(conn, asset_id=None):
    """Insert a new inspection record"""
    global inspection_id_counter
    
    cursor = conn.cursor()
    
    # If no asset_id provided, get a random existing one or create new
    if asset_id is None:
        cursor.execute("SELECT TOP 1 asset_id FROM Assets ORDER BY NEWID()")
        row = cursor.fetchone()
        if row:
            asset_id = row[0]
        else:
            asset_id = insert_asset(conn)
            if asset_id is None:
                return None
    
    inspector_name = random.choice(INSPECTOR_NAMES)
    status = random.choice(INSPECTION_STATUSES)
    
    # Random date for inspection within last 90 days
    inspection_date = datetime.now() - timedelta(days=random.randint(0, 90))
    
    # Random notes
    notes = f"Routine inspection by {inspector_name}. Status: {status}."
    if status == "Failed":
        notes += " Requires immediate attention."
    elif status == "Requires Attention":
        notes += " Minor issues detected."
    
    sql = f"""
    INSERT INTO Inspections (inspection_id, asset_id, inspection_date, inspector_name, status, notes)
    VALUES ({inspection_id_counter}, {asset_id}, '{inspection_date.strftime('%Y-%m-%d')}', 
            '{inspector_name}', '{status}', '{notes}')
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Inserted inspection: {inspection_id_counter} for asset {asset_id}")
        inspection_id_counter += 1
        return inspection_id_counter - 1
    except pyodbc.Error as e:
        logger.error(f"Error inserting inspection: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()


def update_random_location(conn):
    """Update a random location record"""
    cursor = conn.cursor()
    
    # Get a random location ID
    cursor.execute("SELECT TOP 1 location_id FROM Locations ORDER BY NEWID()")
    row = cursor.fetchone()
    if not row:
        cursor.close()
        return
    
    location_id = row[0]
    new_name = random.choice(LOCATION_NAMES) + " Updated " + str(random.randint(100, 999))
    
    sql = f"""
    UPDATE Locations 
    SET location_name = '{new_name}', updated_at = GETDATE() 
    WHERE location_id = {location_id}
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Updated location: {location_id} - new name: {new_name}")
    except pyodbc.Error as e:
        logger.error(f"Error updating location: {e}")
        conn.rollback()
    finally:
        cursor.close()


def update_random_facility(conn):
    """Update a random facility record"""
    cursor = conn.cursor()
    
    # Get a random facility ID
    cursor.execute("SELECT TOP 1 facility_id FROM Facilities ORDER BY NEWID()")
    row = cursor.fetchone()
    if not row:
        cursor.close()
        return
    
    facility_id = row[0]
    new_status = random.choice(FACILITY_STATUSES)
    
    sql = f"""
    UPDATE Facilities 
    SET status = '{new_status}', updated_at = GETDATE() 
    WHERE facility_id = {facility_id}
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Updated facility: {facility_id} - new status: {new_status}")
    except pyodbc.Error as e:
        logger.error(f"Error updating facility: {e}")
        conn.rollback()
    finally:
        cursor.close()


def update_random_asset(conn):
    """Update a random asset record"""
    cursor = conn.cursor()
    
    # Get a random asset ID
    cursor.execute("SELECT TOP 1 asset_id FROM Assets ORDER BY NEWID()")
    row = cursor.fetchone()
    if not row:
        cursor.close()
        return
    
    asset_id = row[0]
    new_maintenance_date = datetime.now().strftime('%Y-%m-%d')
    
    sql = f"""
    UPDATE Assets 
    SET last_maintenance_date = '{new_maintenance_date}', updated_at = GETDATE() 
    WHERE asset_id = {asset_id}
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Updated asset: {asset_id} - new maintenance date: {new_maintenance_date}")
    except pyodbc.Error as e:
        logger.error(f"Error updating asset: {e}")
        conn.rollback()
    finally:
        cursor.close()


def delete_random_inspection(conn):
    """Delete a random inspection record"""
    cursor = conn.cursor()
    
    # Get a random inspection ID
    cursor.execute("SELECT TOP 1 inspection_id FROM Inspections ORDER BY NEWID()")
    row = cursor.fetchone()
    if not row:
        cursor.close()
        return
    
    inspection_id = row[0]
    
    sql = f"DELETE FROM Inspections WHERE inspection_id = {inspection_id}"
    
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Deleted inspection: {inspection_id}")
    except pyodbc.Error as e:
        logger.error(f"Error deleting inspection: {e}")
        conn.rollback()
    finally:
        cursor.close()


def perform_random_operations(conn):
    """Perform a mix of random database operations"""
    # Define weights for different operations
    operations = [
        (insert_location, 10),
        (insert_facility, 10),
        (insert_asset, 15),
        (insert_inspection, 20),
        (update_random_location, 10),
        (update_random_facility, 15),
        (update_random_asset, 15),
        (delete_random_inspection, 5)
    ]
    
    # Choose operations based on weights
    weights = [op[1] for op in operations]
    chosen_operations = random.choices(
        [op[0] for op in operations],
        weights=weights,
        k=random.randint(3, 8)  # Do 3-8 operations per batch
    )
    
    for operation in chosen_operations:
        operation(conn)


def wait_for_database():
    """Wait for the database to become available"""
    logger.info("Waiting for database to become available...")
    while True:
        conn = get_connection()
        if conn:
            conn.close()
            logger.info("Database is available!")
            return
        time.sleep(5)


def main():
    """Main function to run the data generator"""
    logger.info("Starting MS SQL data generator for CDC testing")
    
    # Wait for the database to be ready
    wait_for_database()
    
    # Initial connection
    conn = get_connection()
    if not conn:
        logger.error("Could not connect to database after waiting. Exiting.")
        return
    
    # Initialize counters
    initialize_counters(conn)
    conn.close()
    
    # Main loop
    while True:
        try:
            conn = get_connection()
            if conn:
                perform_random_operations(conn)
                conn.close()
            
            logger.info(f"Sleeping for {INTERVAL_SECONDS} seconds...")
            time.sleep(INTERVAL_SECONDS)
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
