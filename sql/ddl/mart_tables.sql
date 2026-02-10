-- Mart Schema Tables
-- Analytics-ready dimensional models with business logic

-- Drop existing tables
DROP TABLE IF EXISTS mart.inflation_by_state CASCADE;
DROP TABLE IF EXISTS mart.inflation_by_category CASCADE;
DROP TABLE IF EXISTS mart.state_comparison CASCADE;

-- Mart 1: Inflation by State
CREATE TABLE mart.inflation_by_state (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    index_value DECIMAL(10, 4) NOT NULL,
    mom_change DECIMAL(10, 4),  -- Month-over-month % change
    yoy_change DECIMAL(10, 4),  -- Year-over-year % change
    inflation_rate DECIMAL(10, 4),  -- Annual inflation rate
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(state, date)
);

CREATE INDEX idx_mart_inflation_state_date ON mart.inflation_by_state(state, date);
CREATE INDEX idx_mart_inflation_date ON mart.inflation_by_state(date);

-- Mart 2: Inflation by Category
CREATE TABLE mart.inflation_by_category (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    division VARCHAR(10) NOT NULL,
    category_name VARCHAR(200),
    avg_index DECIMAL(10, 4) NOT NULL,  -- Average across all states
    mom_change DECIMAL(10, 4),
    yoy_change DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, division)
);

CREATE INDEX idx_mart_category_date ON mart.inflation_by_category(date);
CREATE INDEX idx_mart_category_division ON mart.inflation_by_category(division);

-- Mart 3: State Comparison (latest month)
CREATE TABLE mart.state_comparison (
    state VARCHAR(100) PRIMARY KEY,
    latest_date DATE NOT NULL,
    overall_cpi DECIMAL(10, 4) NOT NULL,
    food_cpi DECIMAL(10, 4),
    housing_cpi DECIMAL(10, 4),
    transport_cpi DECIMAL(10, 4),
    rank_overall INTEGER,
    pct_vs_cheapest DECIMAL(10, 4),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments
COMMENT ON TABLE mart.inflation_by_state IS 'State-level inflation metrics with MoM and YoY changes';
COMMENT ON TABLE mart.inflation_by_category IS 'Category-level inflation trends (national average)';
COMMENT ON TABLE mart.state_comparison IS 'Latest month state rankings and comparisons';