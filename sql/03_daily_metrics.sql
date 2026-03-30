SELECT
    pickup_date,
    total_trips,
    avg_distance,
    avg_fare,
    avg_duration,
    avg_speed,
    avg_tip_pct,
    total_revenue,
    pct_trips
FROM vw_taxi_daily
ORDER BY pickup_date;

SELECT
    pickup_week_start,
    total_trips,
    avg_distance,
    avg_fare,
    avg_duration,
    avg_speed,
    avg_tip_pct,
    total_revenue,
    pct_trips
FROM vw_taxi_weekly
ORDER BY pickup_week_start;

