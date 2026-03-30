from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

CSV_PATTERN = "yellow_tripdata_*.csv"

RATECODE_VARIANTS = {
    "RatecodeID": "RateCodeID",
}

EXPECTED_MONTHS = {"2015-01", "2016-01", "2016-02", "2016-03"}


def ensure_directories() -> None:
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    output.columns = output.columns.str.strip()
    output = output.rename(columns=RATECODE_VARIANTS)
    return output


def safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    denominator_arr = pd.Series(denominator).replace(0, pd.NA)
    result = pd.Series(numerator) / denominator_arr
    return result.replace([float("inf"), float("-inf")], pd.NA)


def extract_year_month_from_filename(file_name: str) -> str | None:
    match = re.search(r"(\d{4}-\d{2})", file_name)
    return match.group(1) if match else None
