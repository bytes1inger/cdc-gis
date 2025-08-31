-- Create tables for CDC source database
USE cdc_source;
GO

-- Create Locations table
CREATE TABLE Locations (
  location_id INT PRIMARY KEY,
  location_name VARCHAR(100) NOT NULL,
  geom GEOGRAPHY NOT NULL,
  created_at DATETIME DEFAULT GETDATE(),
  updated_at DATETIME DEFAULT GETDATE()
);
GO

-- Create Facilities table
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
GO

-- Create Assets table
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
GO

-- Create Inspections table
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
GO

-- Create spatial indexes for better performance
CREATE SPATIAL INDEX SIndx_Locations_geom ON Locations(geom);
CREATE SPATIAL INDEX SIndx_Facilities_geom ON Facilities(geom);
GO

-- Create regular indexes for foreign keys
CREATE INDEX IX_Facilities_location_id ON Facilities(location_id);
CREATE INDEX IX_Assets_facility_id ON Assets(facility_id);
CREATE INDEX IX_Inspections_asset_id ON Inspections(asset_id);
GO
