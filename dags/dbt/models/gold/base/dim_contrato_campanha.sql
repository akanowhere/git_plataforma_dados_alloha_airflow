SELECT
  id AS id_contrato_campanha,
  id_contrato,
  id_campanha,
  recorrencia_vigencia,
  CASE
    WHEN cancelada = TRUE THEN TRUE
    ELSE FALSE
  END AS cancelada,
  id_venda,
  recorrencia_percentual_desconto,
  recorrencia_meses_desconto,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM {{ ref("vw_air_tbl_contrato_campanha") }}
