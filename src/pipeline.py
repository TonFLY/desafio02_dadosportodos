from __future__ import annotations

import argparse
from pathlib import Path

from src.analytics import (
    build_daily_metrics,
    build_payment_metrics,
    build_shift_metrics,
    build_vendor_metrics,
    build_weekly_metrics,
)
from src.ingest import ingest_raw_data
from src.quality import build_quality_summary, quality_by_rule, split_valid_invalid, apply_quality_rules
from src.transform import transform_base
from src.utils import DATA_PROCESSED_DIR, DATA_RAW_DIR, ensure_directories


def _save_parquet(df, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def run_pipeline(raw_dir: Path = DATA_RAW_DIR, processed_dir: Path = DATA_PROCESSED_DIR, nrows: int | None = None) -> None:
    ensure_directories()

    df_raw = ingest_raw_data(raw_dir=raw_dir, nrows=nrows)
    df_transformed = transform_base(df_raw)
    df_quality = apply_quality_rules(df_transformed)

    df_valid, df_invalid = split_valid_invalid(df_quality)
    df_quality_summary = build_quality_summary(df_quality)
    df_quality_rules = quality_by_rule(df_quality)

    df_daily = build_daily_metrics(df_valid)
    df_weekly = build_weekly_metrics(df_valid)
    df_shift = build_shift_metrics(df_valid)
    df_vendor = build_vendor_metrics(df_valid)
    df_payment = build_payment_metrics(df_valid)

    _save_parquet(df_valid, processed_dir / "taxi_clean.parquet")
    _save_parquet(df_invalid, processed_dir / "taxi_invalid.parquet")
    _save_parquet(df_quality_summary, processed_dir / "taxi_quality_summary.parquet")
    _save_parquet(df_quality_rules, processed_dir / "taxi_quality_by_rule.parquet")

    _save_parquet(df_daily, processed_dir / "taxi_daily.parquet")
    _save_parquet(df_weekly, processed_dir / "taxi_weekly.parquet")
    _save_parquet(df_shift, processed_dir / "taxi_shift.parquet")
    _save_parquet(df_vendor, processed_dir / "taxi_vendor.parquet")
    _save_parquet(df_payment, processed_dir / "taxi_payment_type.parquet")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa pipeline analitica NYC Yellow Taxi.")
    parser.add_argument("--raw-dir", type=Path, default=DATA_RAW_DIR, help="Diretorio com CSVs brutos.")
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=DATA_PROCESSED_DIR,
        help="Diretorio de saida dos Parquets processados.",
    )
    parser.add_argument(
        "--nrows",
        type=int,
        default=None,
        help="Limite de linhas por arquivo (opcional, util para testes).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(raw_dir=args.raw_dir, processed_dir=args.processed_dir, nrows=args.nrows)

