CREATE OR REPLACE VIEW vw_taxi_clean AS
SELECT *
FROM read_parquet('data/processed/taxi_clean.parquet');

CREATE OR REPLACE VIEW vw_taxi_daily AS
SELECT *
FROM read_parquet('data/processed/taxi_daily.parquet');

CREATE OR REPLACE VIEW vw_taxi_weekly AS
SELECT *
FROM read_parquet('data/processed/taxi_weekly.parquet');

CREATE OR REPLACE VIEW vw_taxi_quality_summary AS
SELECT *
FROM read_parquet('data/processed/taxi_quality_summary.parquet');

CREATE OR REPLACE VIEW vw_taxi_quality_by_rule AS
SELECT *
FROM read_parquet('data/processed/taxi_quality_by_rule.parquet');

