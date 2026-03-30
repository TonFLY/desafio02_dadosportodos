from __future__ import annotations

import pandas as pd

from src.utils import EXPECTED_MONTHS

FLAG_COLUMNS = [
    "flag_data_nula",
    "flag_ordem_temporal_invalida",
    "flag_distancia_invalida",
    "flag_passageiros_invalidos",
    "flag_tarifa_invalida",
    "flag_duracao_invalida",
    "flag_velocidade_invalida",
    "flag_vendor_invalido",
    "flag_payment_type_invalido",
    "flag_store_and_fwd_invalido",
    "flag_ratecode_invalido",
    "flag_duplicado",
    "flag_total_inconsistente",
    "flag_pickup_coord_invalida",
    "flag_dropoff_coord_invalida",
    "flag_fora_mes",
]


def apply_quality_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    vendors_validos = {1, 2}
    payment_types_validos = {1, 2, 3, 4, 5, 6}
    store_fwd_validos = {"Y", "N"}
    rate_codes_validos = {1, 2, 3, 4, 5, 6}

    store_and_fwd = out["store_and_fwd_flag"].astype("string").str.upper().str.strip()
    expected_month = out["ano_mes_esperado"]

    out["flag_data_nula"] = out["tpep_pickup_datetime"].isna() | out["tpep_dropoff_datetime"].isna()
    out["flag_ordem_temporal_invalida"] = out["tpep_dropoff_datetime"] < out["tpep_pickup_datetime"]
    out["flag_distancia_invalida"] = out["trip_distance"].isna() | (out["trip_distance"] <= 0) | (out["trip_distance"] > 100)
    out["flag_passageiros_invalidos"] = (
        out["passenger_count"].isna() | (out["passenger_count"] <= 0) | (out["passenger_count"] > 6)
    )
    out["flag_tarifa_invalida"] = out["fare_amount"].isna() | (out["fare_amount"] <= 0) | (out["fare_amount"] > 500)
    out["flag_duracao_invalida"] = (
        out["trip_duration_min"].isna() | (out["trip_duration_min"] <= 0) | (out["trip_duration_min"] > 300)
    )
    out["flag_velocidade_invalida"] = (
        out["avg_speed_mph"].isna() | (out["avg_speed_mph"] <= 0) | (out["avg_speed_mph"] > 85)
    )
    out["flag_vendor_invalido"] = ~out["VendorID"].isin(vendors_validos)
    out["flag_payment_type_invalido"] = ~out["payment_type"].isin(payment_types_validos)
    out["flag_store_and_fwd_invalido"] = ~store_and_fwd.isin(store_fwd_validos)
    out["flag_ratecode_invalido"] = ~out["RateCodeID"].isin(rate_codes_validos)

    out["flag_duplicado"] = out.duplicated(
        subset=[
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ],
        keep=False,
    )

    out["flag_total_inconsistente"] = (out["total_amount"] - out["total_calculado"]).abs() > 0.01

    out["flag_pickup_coord_invalida"] = (
        out["pickup_longitude"].isna()
        | out["pickup_latitude"].isna()
        | (out["pickup_longitude"] < -74.30)
        | (out["pickup_longitude"] > -73.65)
        | (out["pickup_latitude"] < 40.45)
        | (out["pickup_latitude"] > 40.95)
    )

    out["flag_dropoff_coord_invalida"] = (
        out["dropoff_longitude"].isna()
        | out["dropoff_latitude"].isna()
        | (out["dropoff_longitude"] < -74.30)
        | (out["dropoff_longitude"] > -73.65)
        | (out["dropoff_latitude"] < 40.45)
        | (out["dropoff_latitude"] > 40.95)
    )

    out["flag_fora_mes"] = (
        expected_month.isna()
        | ~expected_month.isin(EXPECTED_MONTHS)
        | (out["pickup_year_month"] != expected_month)
    )

    out[FLAG_COLUMNS] = out[FLAG_COLUMNS].fillna(False)
    out["qtd_regras_invalidas"] = out[FLAG_COLUMNS].astype(int).sum(axis=1)
    out["flag_invalido"] = out["qtd_regras_invalidas"] > 0

    return out


def split_valid_invalid(df_quality: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid = df_quality[~df_quality["flag_invalido"]].copy()
    invalid = df_quality[df_quality["flag_invalido"]].copy()
    return valid, invalid


def build_quality_summary(df_quality: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df_quality.groupby("arquivo_origem", dropna=False)
        .agg(
            total_registros=("arquivo_origem", "size"),
            registros_invalidos=("flag_invalido", "sum"),
            media_regras_invalidas=("qtd_regras_invalidas", "mean"),
        )
        .reset_index()
    )

    grouped["registros_validos"] = grouped["total_registros"] - grouped["registros_invalidos"]
    grouped["pct_invalidos"] = 0.0
    mask_total = grouped["total_registros"] > 0
    grouped.loc[mask_total, "pct_invalidos"] = (
        grouped.loc[mask_total, "registros_invalidos"] / grouped.loc[mask_total, "total_registros"] * 100
    )
    grouped["score_qualidade"] = 100 - grouped["pct_invalidos"]

    for flag in FLAG_COLUMNS:
        counts = df_quality.groupby("arquivo_origem", dropna=False)[flag].sum().reset_index(name=flag)
        grouped = grouped.merge(counts, on="arquivo_origem", how="left")

    global_row = {
        "arquivo_origem": "__GLOBAL__",
        "total_registros": int(len(df_quality)),
        "registros_invalidos": int(df_quality["flag_invalido"].sum()),
        "media_regras_invalidas": float(df_quality["qtd_regras_invalidas"].mean()),
    }
    global_row["registros_validos"] = global_row["total_registros"] - global_row["registros_invalidos"]
    global_row["pct_invalidos"] = (
        (global_row["registros_invalidos"] / global_row["total_registros"]) * 100
        if global_row["total_registros"] > 0
        else 0.0
    )
    global_row["score_qualidade"] = 100 - global_row["pct_invalidos"]
    for flag in FLAG_COLUMNS:
        global_row[flag] = int(df_quality[flag].sum())

    return pd.concat([grouped, pd.DataFrame([global_row])], ignore_index=True)


def quality_by_rule(df_quality: pd.DataFrame) -> pd.DataFrame:
    total = len(df_quality)
    rule_stats = []
    for flag in FLAG_COLUMNS:
        invalid_count = int(df_quality[flag].sum())
        rule_stats.append(
            {
                "regra": flag,
                "invalid_count": invalid_count,
                "invalid_pct": (invalid_count / total * 100) if total else 0.0,
            }
        )
    out = pd.DataFrame(rule_stats).sort_values("invalid_count", ascending=False).reset_index(drop=True)
    return out
