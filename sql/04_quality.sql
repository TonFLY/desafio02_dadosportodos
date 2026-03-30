SELECT
    arquivo_origem,
    total_registros,
    registros_validos,
    registros_invalidos,
    pct_invalidos,
    score_qualidade,
    media_regras_invalidas
FROM vw_taxi_quality_summary
ORDER BY arquivo_origem;

SELECT
    regra,
    invalid_count,
    invalid_pct
FROM vw_taxi_quality_by_rule
ORDER BY invalid_count DESC;

