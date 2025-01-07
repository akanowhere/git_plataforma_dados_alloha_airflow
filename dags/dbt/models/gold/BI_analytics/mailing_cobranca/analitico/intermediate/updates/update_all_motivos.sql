{{
    config(
        materialized='ephemeral'
    )
}}


WITH motivos_contratos AS (
    SELECT *
    FROM {{ ref('update_motivos_contratos') }}
)
, motivos_faturas AS (
    SELECT *
    FROM {{ ref('update_motivos_faturas') }}
    WHERE fatura_motivadora NOT IN (SELECT fatura_motivadora FROM motivos_contratos)
)
, all_motivos AS (
    SELECT *
    FROM(
        SELECT * FROM motivos_contratos
        UNION ALL   
        SELECT * FROM motivos_faturas
    )
)
SELECT *
FROM all_motivos