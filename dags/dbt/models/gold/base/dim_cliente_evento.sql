SELECT
  id AS id_evento,
  CAST(momento AS TIMESTAMP) AS momento,
  usuario,
  id_contrato,
  id_cliente,
  tipo,
  observacao,
  motivo_associado,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM {{ ref('vw_air_tbl_cliente_evento') }}