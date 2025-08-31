-- Create CDC source database
USE master;
GO

-- Drop database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'cdc_source')
BEGIN
    ALTER DATABASE cdc_source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE cdc_source;
END
GO

-- Create the database
CREATE DATABASE cdc_source;
GO

-- Use the new database
USE cdc_source;
GO

-- Enable CDC at the database level
EXEC sys.sp_cdc_enable_db;
GO

-- Verify CDC is enabled
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = 'cdc_source';
GO
