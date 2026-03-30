WITH base AS (
    SELECT * FROM vw_taxi_clean
)
SELECT
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_duration_min) AS avg_duration,
    AVG(avg_speed_mph) AS avg_speed,
    AVG(tip_pct) AS avg_tip_pct,
    SUM(total_amount) AS total_revenue
FROM base;

