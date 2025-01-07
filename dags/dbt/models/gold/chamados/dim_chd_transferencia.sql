WITH vw_air_tbl_chd_transferencia AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_transferencia') }}
)

SELECT
  id,
  data_transferencia,
  id_chamado,
  codigo_usuario,
  id_fila_origem,
  id_fila_destino

FROM vw_air_tbl_chd_transferencia
