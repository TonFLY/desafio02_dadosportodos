from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils import CSV_PATTERN, DATA_RAW_DIR, normalize_columns


def discover_csv_files(raw_dir: Path = DATA_RAW_DIR, pattern: str = CSV_PATTERN) -> list[Path]:
    files = sorted(raw_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em '{raw_dir}' com pattern '{pattern}'."
        )
    return files


def read_taxi_csv(file_path: Path, nrows: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(file_path, low_memory=False, nrows=nrows)
    df = normalize_columns(df)
    df["arquivo_origem"] = file_path.name
    return df


def ingest_raw_data(raw_dir: Path = DATA_RAW_DIR, nrows: int | None = None) -> pd.DataFrame:
    files = discover_csv_files(raw_dir=raw_dir)
    frames = [read_taxi_csv(file_path=csv_file, nrows=nrows) for csv_file in files]
    combined = pd.concat(frames, ignore_index=True)
    return normalize_columns(combined)

