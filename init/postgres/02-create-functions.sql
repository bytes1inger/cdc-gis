-- Create useful PostGIS functions for geographic data analysis

-- Function to calculate distance between facilities
CREATE OR REPLACE FUNCTION calculate_facility_distances()
RETURNS TABLE (
    facility_id_a INTEGER,
    facility_name_a VARCHAR(100),
    facility_id_b INTEGER,
    facility_name_b VARCHAR(100),
    distance_meters DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.facility_id AS facility_id_a,
        a.facility_name AS facility_name_a,
        b.facility_id AS facility_id_b,
        b.facility_name AS facility_name_b,
        ST_Distance(a.geom::geography, b.geom::geography) AS distance_meters
    FROM 
        "Facilities" a
    CROSS JOIN 
        "Facilities" b
    WHERE 
        a.facility_id < b.facility_id
    ORDER BY 
        distance_meters;
END;
$$ LANGUAGE plpgsql;

-- Function to find facilities within a given distance of a point
CREATE OR REPLACE FUNCTION find_facilities_within_distance(
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION
)
RETURNS TABLE (
    facility_id INTEGER,
    facility_name VARCHAR(100),
    facility_type VARCHAR(50),
    distance DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.facility_id,
        f.facility_name,
        f.facility_type,
        ST_Distance(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) AS distance
    FROM 
        "Facilities" f
    WHERE 
        ST_DWithin(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, distance_meters)
    ORDER BY 
        distance;
END;
$$ LANGUAGE plpgsql;

-- Function to create a buffer around locations and find all facilities within it
CREATE OR REPLACE FUNCTION find_facilities_in_location_buffer(
    location_id_param INTEGER,
    buffer_meters DOUBLE PRECISION
)
RETURNS TABLE (
    facility_id INTEGER,
    facility_name VARCHAR(100),
    facility_type VARCHAR(50),
    distance_from_location DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.facility_id,
        f.facility_name,
        f.facility_type,
        ST_Distance(f.geom::geography, l.geom::geography) AS distance_from_location
    FROM 
        "Facilities" f,
        "Locations" l
    WHERE 
        l.location_id = location_id_param AND
        ST_DWithin(f.geom::geography, l.geom::geography, buffer_meters)
    ORDER BY 
        distance_from_location;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate asset statistics by facility
CREATE OR REPLACE FUNCTION get_asset_stats_by_facility()
RETURNS TABLE (
    facility_id INTEGER,
    facility_name VARCHAR(100),
    total_assets INTEGER,
    assets_needing_attention INTEGER,
    assets_passed INTEGER,
    inspection_coverage_percent NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.facility_id,
        f.facility_name,
        COUNT(a.asset_id) AS total_assets,
        COUNT(CASE WHEN ais.current_status = 'Needs Attention' THEN 1 ELSE NULL END) AS assets_needing_attention,
        COUNT(CASE WHEN ais.inspection_status = 'Passed' THEN 1 ELSE NULL END) AS assets_passed,
        ROUND((COUNT(i.inspection_id)::NUMERIC / NULLIF(COUNT(a.asset_id), 0)::NUMERIC) * 100, 2) AS inspection_coverage_percent
    FROM 
        "Facilities" f
    LEFT JOIN 
        "Assets" a ON f.facility_id = a.facility_id
    LEFT JOIN 
        asset_inspection_status ais ON a.asset_id = ais.asset_id
    LEFT JOIN 
        "Inspections" i ON a.asset_id = i.asset_id
    GROUP BY 
        f.facility_id, f.facility_name
    ORDER BY 
        f.facility_id;
END;
$$ LANGUAGE plpgsql;
