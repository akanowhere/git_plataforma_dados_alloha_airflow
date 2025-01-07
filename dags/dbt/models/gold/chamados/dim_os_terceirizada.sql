WITH vw_air_tbl_os_terceirizada AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_os_terceirizada') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  codigo_usuario,
  nome,
  posicao_deposito_sap,
  ativo,
  razao_social,
  cnpj

FROM vw_air_tbl_os_terceirizada
