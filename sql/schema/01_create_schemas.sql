-- Create schemas for our data warehouse
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS mart CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA mart;

-- Create metadata table to track data loads
CREATE TABLE raw.load_metadata (
    load_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_loaded INTEGER,
    load_status VARCHAR(20) DEFAULT 'SUCCESS',
    error_message TEXT,
    source_file VARCHAR(255)
);

-- Add comments
COMMENT ON SCHEMA raw IS 'Raw data exactly as extracted from source';
COMMENT ON SCHEMA staging IS 'Cleaned and standardized data';
COMMENT ON SCHEMA mart IS 'Analytics-ready dimensional models';

-- Log schema creation
INSERT INTO raw.load_metadata (table_name, records_loaded, load_status)
VALUES ('schema_initialization', 3, 'SUCCESS');

-- Display created schemas
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'staging', 'mart');