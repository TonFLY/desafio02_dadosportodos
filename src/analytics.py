from __future__ import annotations

import pandas as pd


def _metrics_by_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            total_trips=("arquivo_origem", "size"),
            avg_distance=("trip_distance", "mean"),
            avg_fare=("fare_amount", "mean"),
            avg_duration=("trip_duration_min", "mean"),
            avg_speed=("avg_speed_mph", "mean"),
            avg_tip_pct=("tip_pct", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .reset_index()
    )

    total_trips_base = grouped["total_trips"].sum()
    grouped["pct_trips"] = 0.0
    if total_trips_base > 0:
        grouped["pct_trips"] = grouped["total_trips"] / total_trips_base * 100
    return grouped


def build_daily_metrics(df_clean: pd.DataFrame) -> pd.DataFrame:
    return _metrics_by_group(df_clean, ["pickup_date"]).sort_values("pickup_date")


def build_weekly_metrics(df_clean: pd.DataFrame) -> pd.DataFrame:
    out = df_clean.copy()
    out["pickup_week_start"] = (
        out["pickup_date"] - pd.to_timedelta(out["pickup_date"].dt.dayofweek, unit="D")
    )
    return _metrics_by_group(out, ["pickup_week_start"]).sort_values("pickup_week_start")


def build_shift_metrics(df_clean: pd.DataFrame) -> pd.DataFrame:
    return _metrics_by_group(df_clean, ["shift_of_day"]).sort_values("total_trips", ascending=False)


def build_payment_metrics(df_clean: pd.DataFrame) -> pd.DataFrame:
    return _metrics_by_group(df_clean, ["payment_type"]).sort_values("total_trips", ascending=False)


def build_vendor_metrics(df_clean: pd.DataFrame) -> pd.DataFrame:
    return _metrics_by_group(df_clean, ["VendorID"]).sort_values("total_trips", ascending=False)
