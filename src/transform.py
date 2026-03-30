from __future__ import annotations

import pandas as pd

from src.utils import extract_year_month_from_filename, safe_divide


def _shift_from_hour(hour: float | int | None) -> str:
    if pd.isna(hour):
        return "unknown"
    hour = int(hour)
    if 5 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 16:
        return "afternoon"
    if 17 <= hour <= 21:
        return "evening"
    return "night"


def transform_base(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["tpep_pickup_datetime"] = pd.to_datetime(
        out["tpep_pickup_datetime"], errors="coerce"
    )
    out["tpep_dropoff_datetime"] = pd.to_datetime(
        out["tpep_dropoff_datetime"], errors="coerce"
    )

    numeric_cols = [
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "RateCodeID",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "VendorID",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["pickup_date"] = out["tpep_pickup_datetime"].dt.normalize()
    out["dropoff_date"] = out["tpep_dropoff_datetime"].dt.normalize()
    out["pickup_year"] = out["tpep_pickup_datetime"].dt.year
    out["pickup_month"] = out["tpep_pickup_datetime"].dt.month
    out["pickup_day"] = out["tpep_pickup_datetime"].dt.day
    out["pickup_hour"] = out["tpep_pickup_datetime"].dt.hour
    out["pickup_year_month"] = out["tpep_pickup_datetime"].dt.strftime("%Y-%m")

    out["trip_duration_min"] = (
        out["tpep_dropoff_datetime"] - out["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    out["trip_duration_hr"] = out["trip_duration_min"] / 60
    out["avg_speed_mph"] = safe_divide(out["trip_distance"], out["trip_duration_hr"])

    out["tip_pct"] = safe_divide(out["tip_amount"] * 100, out["fare_amount"])
    out["revenue_per_minute"] = safe_divide(out["total_amount"], out["trip_duration_min"])
    out["revenue_per_mile"] = safe_divide(out["total_amount"], out["trip_distance"])

    out["day_of_week"] = out["tpep_pickup_datetime"].dt.day_name()
    out["is_weekend"] = out["tpep_pickup_datetime"].dt.weekday >= 5
    out["shift_of_day"] = out["pickup_hour"].apply(_shift_from_hour)

    out["distance_band"] = pd.cut(
        out["trip_distance"],
        bins=[0, 1, 3, 10, 30, float("inf")],
        labels=["0-1", "1-3", "3-10", "10-30", "30+"],
        include_lowest=True,
    ).astype("string")

    out["duration_band"] = pd.cut(
        out["trip_duration_min"],
        bins=[0, 5, 15, 30, 60, 120, 300, float("inf")],
        labels=["0-5", "5-15", "15-30", "30-60", "60-120", "120-300", "300+"],
        include_lowest=True,
    ).astype("string")

    out["total_calculado"] = (
        out["fare_amount"].fillna(0)
        + out["extra"].fillna(0)
        + out["mta_tax"].fillna(0)
        + out["tip_amount"].fillna(0)
        + out["tolls_amount"].fillna(0)
        + out["improvement_surcharge"].fillna(0)
    )

    out["ano_mes_esperado"] = out["arquivo_origem"].map(extract_year_month_from_filename)

    return out

