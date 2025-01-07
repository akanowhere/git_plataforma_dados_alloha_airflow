{{
    config(
        materialized='table'
    )
}}

{% set dates = get_dates(['first_day_month']) %}

WITH mailing_serasa AS (
  SELECT 
    id_contrato,
    cpf_cnpj,
    marca,
    TRIM(fatura) AS fatura,
    data_referencia
  FROM (
    SELECT 
      id_contrato,
      cpf_cnpj,
      marca,
      data_referencia,
      EXPLODE(SPLIT(faturas_abertas, ',')) AS fatura
    FROM 
      {{ ref('tbl_analitico' )}}
    WHERE
      data_referencia = '{{ dates.first_day_month }}'
) AS exploded_faturas
)
SELECT 
  *
FROM mailing_serasa