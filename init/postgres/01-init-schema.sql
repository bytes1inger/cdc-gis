-- Initialize PostgreSQL target database schema

-- Create extensions
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create Locations table
CREATE TABLE IF NOT EXISTS "Locations" (
  location_id INTEGER PRIMARY KEY,
  location_name VARCHAR(100) NOT NULL,
  geom GEOGRAPHY NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Create Facilities table
CREATE TABLE IF NOT EXISTS "Facilities" (
  facility_id INTEGER PRIMARY KEY,
  facility_name VARCHAR(100) NOT NULL,
  facility_type VARCHAR(50) NOT NULL,
  location_id INTEGER REFERENCES "Locations"(location_id),
  geom GEOGRAPHY NOT NULL,
  status VARCHAR(20) DEFAULT 'Active',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Create Assets table
CREATE TABLE IF NOT EXISTS "Assets" (
  asset_id INTEGER PRIMARY KEY,
  asset_name VARCHAR(100) NOT NULL,
  asset_type VARCHAR(50) NOT NULL,
  facility_id INTEGER REFERENCES "Facilities"(facility_id),
  installation_date DATE,
  last_maintenance_date DATE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Create Inspections table
CREATE TABLE IF NOT EXISTS "Inspections" (
  inspection_id INTEGER PRIMARY KEY,
  asset_id INTEGER REFERENCES "Assets"(asset_id),
  inspection_date DATE NOT NULL,
  inspector_name VARCHAR(100) NOT NULL,
  status VARCHAR(20) NOT NULL,
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Create spatial indexes
CREATE INDEX IF NOT EXISTS "idx_locations_geom" ON "Locations" USING GIST (geom);
CREATE INDEX IF NOT EXISTS "idx_facilities_geom" ON "Facilities" USING GIST (geom);

-- Create regular indexes for foreign keys
CREATE INDEX IF NOT EXISTS "idx_facilities_location_id" ON "Facilities"(location_id);
CREATE INDEX IF NOT EXISTS "idx_assets_facility_id" ON "Assets"(facility_id);
CREATE INDEX IF NOT EXISTS "idx_inspections_asset_id" ON "Inspections"(asset_id);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Create triggers to automatically update the updated_at column
CREATE TRIGGER update_locations_modtime
BEFORE UPDATE ON "Locations"
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_facilities_modtime
BEFORE UPDATE ON "Facilities"
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_assets_modtime
BEFORE UPDATE ON "Assets"
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_inspections_modtime
BEFORE UPDATE ON "Inspections"
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Create a view for spatial analysis
CREATE OR REPLACE VIEW facility_locations AS
SELECT 
    f.facility_id,
    f.facility_name,
    f.facility_type,
    f.status,
    l.location_name,
    l.location_id,
    f.geom as facility_geom,
    l.geom as location_geom
FROM 
    "Facilities" f
JOIN 
    "Locations" l ON f.location_id = l.location_id;

-- Create a view for asset inspection status
CREATE OR REPLACE VIEW asset_inspection_status AS
SELECT 
    a.asset_id,
    a.asset_name,
    a.asset_type,
    f.facility_id,
    f.facility_name,
    i.inspection_id,
    i.inspection_date,
    i.status as inspection_status,
    i.inspector_name,
    a.last_maintenance_date,
    CASE 
        WHEN i.inspection_date IS NULL THEN 'Never Inspected'
        WHEN i.status = 'Failed' THEN 'Needs Attention'
        WHEN i.status = 'Requires Attention' THEN 'Needs Attention'
        WHEN DATE_PART('day', NOW() - i.inspection_date) > 180 THEN 'Inspection Due'
        ELSE 'Good'
    END as current_status
FROM 
    "Assets" a
LEFT JOIN 
    "Facilities" f ON a.facility_id = f.facility_id
LEFT JOIN 
    "Inspections" i ON a.asset_id = i.asset_id
WHERE 
    i.inspection_id IS NULL OR 
    i.inspection_id = (
        SELECT MAX(inspection_id) 
        FROM "Inspections" 
        WHERE asset_id = a.asset_id
    );

-- Grant permissions
ALTER TABLE "Locations" OWNER TO postgres;
ALTER TABLE "Facilities" OWNER TO postgres;
ALTER TABLE "Assets" OWNER TO postgres;
ALTER TABLE "Inspections" OWNER TO postgres;
