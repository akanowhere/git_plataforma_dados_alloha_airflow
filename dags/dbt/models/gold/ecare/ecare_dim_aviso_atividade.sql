{{
  config(
    alias = 'dim_aviso_atividade'
    )
}}

WITH vw_air_tbl_aviso_atividade AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_aviso_atividade') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  id_aviso,
  descricao,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM vw_air_tbl_aviso_atividade
