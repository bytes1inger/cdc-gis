-- Enable CDC on tables
USE cdc_source;
GO

-- Enable CDC on Locations table
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'Locations',
@role_name = NULL,
@supports_net_changes = 1;
GO

-- Enable CDC on Facilities table
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'Facilities',
@role_name = NULL,
@supports_net_changes = 1;
GO

-- Enable CDC on Assets table
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'Assets',
@role_name = NULL,
@supports_net_changes = 1;
GO

-- Enable CDC on Inspections table
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'Inspections',
@role_name = NULL,
@supports_net_changes = 1;
GO

-- Verify CDC is enabled on tables
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE is_tracked_by_cdc = 1;
GO

-- Insert initial sample data for Locations
INSERT INTO Locations (location_id, location_name, geom)
VALUES 
(1, 'Central Park', geography::STGeomFromText('POINT(-73.9654 40.7829)', 4326)),
(2, 'Golden Gate Park', geography::STGeomFromText('POINT(-122.4869 37.7694)', 4326)),
(3, 'Yellowstone National Park', geography::STGeomFromText('POINT(-110.5885 44.4280)', 4326)),
(4, 'Grand Canyon', geography::STGeomFromText('POINT(-112.1401 36.0544)', 4326)),
(5, 'Everglades National Park', geography::STGeomFromText('POINT(-80.9000 25.2866)', 4326));
GO

-- Insert initial sample data for Facilities
INSERT INTO Facilities (facility_id, facility_name, facility_type, location_id, geom, status)
VALUES 
(1, 'Central Park Visitor Center', 'Visitor Center', 1, geography::STGeomFromText('POINT(-73.9708 40.7828)', 4326), 'Active'),
(2, 'Golden Gate Park Observatory', 'Observatory', 2, geography::STGeomFromText('POINT(-122.4732 37.7701)', 4326), 'Active'),
(3, 'Yellowstone Lodge', 'Accommodation', 3, geography::STGeomFromText('POINT(-110.5784 44.4597)', 4326), 'Active'),
(4, 'Grand Canyon Visitor Center', 'Visitor Center', 4, geography::STGeomFromText('POINT(-112.1219 36.0579)', 4326), 'Active'),
(5, 'Everglades Research Station', 'Research', 5, geography::STGeomFromText('POINT(-80.9112 25.2911)', 4326), 'Active');
GO

-- Insert initial sample data for Assets
INSERT INTO Assets (asset_id, asset_name, asset_type, facility_id, installation_date, last_maintenance_date)
VALUES 
(1, 'Information Kiosk 1', 'Kiosk', 1, '2020-01-15', '2023-03-10'),
(2, 'Telescope A', 'Telescope', 2, '2019-06-22', '2023-02-15'),
(3, 'HVAC System', 'HVAC', 3, '2018-11-05', '2023-01-20'),
(4, 'Interactive Display', 'Display', 4, '2021-03-18', '2023-04-05'),
(5, 'Water Quality Sensor', 'Sensor', 5, '2022-02-28', '2023-03-28');
GO

-- Insert initial sample data for Inspections
INSERT INTO Inspections (inspection_id, asset_id, inspection_date, inspector_name, status, notes)
VALUES 
(1, 1, '2023-01-15', 'John Smith', 'Passed', 'All systems functioning normally'),
(2, 2, '2023-02-10', 'Maria Garcia', 'Passed', 'Minor calibration needed'),
(3, 3, '2023-01-05', 'David Johnson', 'Requires Attention', 'Filter replacement needed'),
(4, 4, '2023-03-20', 'Sarah Williams', 'Passed', 'Software updated to latest version'),
(5, 5, '2023-03-15', 'Robert Brown', 'Passed', 'Calibration performed');
GO
