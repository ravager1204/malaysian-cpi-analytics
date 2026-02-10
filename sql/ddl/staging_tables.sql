-- Staging Schema Tables
-- Clean, standardized data ready for transformations

-- Drop existing tables
DROP TABLE IF EXISTS staging.cpi_monthly CASCADE;
DROP TABLE IF EXISTS staging.categories CASCADE;
DROP TABLE IF EXISTS staging.states CASCADE;

-- Staging CPI data with enrichments
CREATE TABLE staging.cpi_monthly (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    division VARCHAR(10) NOT NULL,
    category_name VARCHAR(200),
    index_value DECIMAL(10, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(state, date, division)
);

CREATE INDEX idx_stg_cpi_date ON staging.cpi_monthly(date);
CREATE INDEX idx_stg_cpi_state ON staging.cpi_monthly(state);
CREATE INDEX idx_stg_cpi_division ON staging.cpi_monthly(division);

-- Staging categories lookup
CREATE TABLE staging.categories (
    division VARCHAR(10) PRIMARY KEY,
    category_name_en VARCHAR(200),
    category_name_bm VARCHAR(200),
    category_level INTEGER
);

-- Staging states reference
CREATE TABLE staging.states (
    state_code VARCHAR(10) PRIMARY KEY,
    state_name VARCHAR(100) UNIQUE NOT NULL,
    region VARCHAR(50)
);

-- Add comments
COMMENT ON TABLE staging.cpi_monthly IS 'Cleaned CPI data with category names joined';
COMMENT ON TABLE staging.categories IS 'Category lookup table (2-digit level)';
COMMENT ON TABLE staging.states IS 'State reference data with regional grouping';

-- Insert state reference data
INSERT INTO staging.states (state_code, state_name, region) VALUES
    ('JHR', 'Johor', 'Southern'),
    ('KDH', 'Kedah', 'Northern'),
    ('KTN', 'Kelantan', 'East Coast'),
    ('MLK', 'Melaka', 'Southern'),
    ('NSN', 'Negeri Sembilan', 'Southern'),
    ('PHG', 'Pahang', 'East Coast'),
    ('PNG', 'Pulau Pinang', 'Northern'),
    ('PRK', 'Perak', 'Northern'),
    ('PLS', 'Perlis', 'Northern'),
    ('SGR', 'Selangor', 'Central'),
    ('TRG', 'Terengganu', 'East Coast'),
    ('SBH', 'Sabah', 'East Malaysia'),
    ('SWK', 'Sarawak', 'East Malaysia'),
    ('KUL', 'W.P. Kuala Lumpur', 'Central'),
    ('LBN', 'W.P. Labuan', 'East Malaysia'),
    ('PJY', 'W.P. Putrajaya', 'Central');