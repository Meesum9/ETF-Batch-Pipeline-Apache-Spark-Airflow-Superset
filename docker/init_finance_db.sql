-- Finance Gold Database Schema
-- Creates the database and table for storing ETF KPIs from the Gold layer

-- Create the finance_gold database if it doesn't exist
SELECT 'CREATE DATABASE finance_gold' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'finance_gold')\gexec

-- Connect to the finance_gold database
\c finance_gold;

-- Drop table if it exists (for clean rebuilds)
DROP TABLE IF EXISTS etf_kpis;

-- Create the etf_kpis table for Gold layer data
CREATE TABLE etf_kpis (
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    daily_return_pct DECIMAL(8,4),
    monthly_return_pct DECIMAL(8,4),
    ma_50 DECIMAL(10,4),
    ma_200 DECIMAL(10,4),
    ma_signal VARCHAR(10),
    avg_volume_30d BIGINT,
    volume_spike BOOLEAN,
    high_low_range_pct DECIMAL(8,4),
    ingest_ts TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

-- Create indexes for better query performance
CREATE INDEX idx_etf_kpis_date ON etf_kpis(date);
CREATE INDEX idx_etf_kpis_ticker ON etf_kpis(ticker);
CREATE INDEX idx_etf_kpis_date_ticker ON etf_kpis(date, ticker);

-- Grant permissions to the airflow user
GRANT ALL PRIVILEGES ON DATABASE finance_gold TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Insert sample data for testing (optional)
-- This can be removed in production
INSERT INTO etf_kpis (date, ticker, close, daily_return_pct, ma_signal) VALUES
('2024-01-15', 'XLK', 150.25, 0.5, 'bullish'),
('2024-01-15', 'XLF', 85.75, -0.3, 'bearish'),
('2024-01-15', 'XLE', 65.50, 1.2, 'bullish')
ON CONFLICT (date, ticker) DO NOTHING;
