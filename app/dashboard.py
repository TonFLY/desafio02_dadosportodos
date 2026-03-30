from __future__ import annotations

from pathlib import Path

import duckdb
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def _query(sql: str):
    with duckdb.connect() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW vw_taxi_clean AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_clean.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_daily AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_daily.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_weekly AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_weekly.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_quality_summary AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_quality_summary.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_quality_by_rule AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_quality_by_rule.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_shift AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_shift.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_vendor AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_vendor.parquet").as_posix()}');
            CREATE OR REPLACE VIEW vw_taxi_payment_type AS
            SELECT * FROM read_parquet('{(PROCESSED_DIR / "taxi_payment_type.parquet").as_posix()}');
            """
        )
        return conn.execute(sql).df()


def _ensure_data():
    required = [
        "taxi_clean.parquet",
        "taxi_daily.parquet",
        "taxi_weekly.parquet",
        "taxi_quality_summary.parquet",
        "taxi_quality_by_rule.parquet",
        "taxi_shift.parquet",
        "taxi_vendor.parquet",
        "taxi_payment_type.parquet",
    ]
    missing = [name for name in required if not (PROCESSED_DIR / name).exists()]
    if missing:
        st.error("Arquivos processados nao encontrados. Rode primeiro: python -m src.pipeline")
        st.write("Arquivos ausentes:", missing)
        st.stop()


def main():
    st.set_page_config(page_title="NYC Yellow Taxi Analytics", layout="wide")
    st.title("NYC Yellow Taxi | Dashboard Analitico")
    st.caption("Dados Yellow Taxi NYC | Janeiro de 2015 e Janeiro à Marco de 2016")

    _ensure_data()

    st.header("Visao geral")
    kpis = _query(
        """
        SELECT
                COUNT(*)                    AS 'Total de viagens'
            ,   AVG(trip_distance)          AS 'Distancia media (mi)'
            ,   AVG(fare_amount)            AS 'Tarifa media ($)'
            ,   AVG(trip_duration_min)      AS 'Duracao media (min)'
            ,   AVG(avg_speed_mph)          AS 'Velocidade media (mph)'
            ,   AVG(tip_pct)                AS 'Pct de gorjeta'
            ,   SUM(total_amount)           AS 'Receita total ($)'
        FROM vw_taxi_clean
        """
    )

    cols = st.columns(4)
    cols[0].metric("Total de viagens", f"{int(kpis.loc[0, 'Total de viagens']):,}")
    cols[1].metric("Distancia media (mi)", f"{kpis.loc[0, 'Distancia media (mi)']:.2f}")
    cols[2].metric("Tarifa media ($)", f"{kpis.loc[0, 'Tarifa media ($)']:.2f}")
    cols[3].metric("Receita total ($)", f"{kpis.loc[0, 'Receita total ($)']:.2f}")

    st.header("Qualidade dos dados")
    st.caption("Essa sesao dos dados é apenas para demostrar a qualidade dos dados em um cenario real não é necessario expor essas métricas para os usuários finais, mas é uma prática recomendada monitorar a qualidade dos dados em produção para detectar e corrigir problemas rapidamente.")
    quality_summary = _query(
        """
        SELECT 
                arquivo_origem                  as 'Arquivo de origem'
            ,   total_registros                 as 'Total de registros'
            ,   registros_validos               as 'Registros validos'
            ,   registros_invalidos             as 'Registros invalidos'
            ,   round(pct_invalidos, 2) ||'%'   as 'Pct de registros invalidos (%)'
            ,   round(score_qualidade, 2) ||'%' as 'Score de qualidade (%)'
        FROM vw_taxi_quality_summary 
        ORDER BY arquivo_origem
        """
    )
    st.dataframe(quality_summary, use_container_width=True)

    quality_rules = _query(
        """
        SELECT 
                    regra
                ,   invalid_count         AS 'Contagem de registros invalidos'
                ,   invalid_pct ||'%'     AS 'Pct de registros invalidos (%)'
        FROM vw_taxi_quality_by_rule
        ORDER BY invalid_count DESC
        """
    )
    st.bar_chart(quality_rules.set_index("regra")["Contagem de registros invalidos"])

    st.header("Analise temporal")
    daily = _query("SELECT pickup_date as 'Data', total_trips as 'Total de viagens', total_revenue as 'Receita total' FROM vw_taxi_daily ORDER BY pickup_date")
    st.line_chart(daily.set_index("Data")[["Total de viagens", "Receita total"]])

    weekly = _query("SELECT pickup_week_start as 'Inicio da semana', avg_fare as 'Tarifa media', avg_distance as 'Distancia media' FROM vw_taxi_weekly ORDER BY pickup_week_start")
    st.line_chart(weekly.set_index("Inicio da semana")[["Tarifa media", "Distancia media"]])

    st.header("Receita e tarifa")
    payment = _query(
        """
        SELECT 
                    CASE payment_type
                        WHEN 1 THEN 'Cartão de credito'
                        WHEN 2 THEN 'Dinheiro'
                        WHEN 3 THEN 'Sem cobrança'
                        WHEN 4 THEN 'Disputa'
                        WHEN 6 THEN 'Cancelada'
                        ELSE 'Desconhecido'
                    END             AS 'Tipo de pagamento'
                ,   total_trips     as 'Total de viagens'
                ,   total_revenue   as 'Receita total ($)'
                ,   avg_fare        as 'Tarifa media ($)'
        FROM vw_taxi_payment_type
        ORDER BY total_revenue DESC
        """
    )
    st.dataframe(payment, use_container_width=True)

    st.header("Operacao por vendor e turno")
    vendor = _query(
        """
        SELECT 
                    CASE VendorID
                        WHEN 1 THEN 'Creative Mobile Technologies (CMT)'
                        WHEN 2 THEN 'VeriFone Inc.'
                        ELSE 'Unknown'
                    END             AS 'Vendor'
                ,   total_trips     as 'Total de viagens'
                ,   total_revenue   as 'Receita total ($)'
                ,   avg_speed       as 'Velocidade media (mph)'
        FROM vw_taxi_vendor
        ORDER BY total_revenue DESC
        """
    )
    st.dataframe(vendor, use_container_width=True)

    shift = _query(
        """
        SELECT 
                CASE shift_of_day 
                    WHEN 'morning'      THEN 'Manhã'
                    WHEN 'afternoon'    THEN 'Tarde'
                    WHEN 'evening'      THEN 'Noite'
                    WHEN 'night'        THEN 'Madrugada'
                    ELSE 'Não mapeado'
                END                         AS 'Turno'
                ,   total_trips             AS 'Total de viagens'
                ,   avg_duration            AS 'Duracao media (min)'
                ,   total_revenue           AS 'Receita total ($)'
                ,   avg_speed               AS 'Velocidade media (mph)'
        FROM vw_taxi_shift
        ORDER BY total_trips DESC
        """
    )
    st.dataframe(shift, use_container_width=True)


if __name__ == "__main__":
    main()

