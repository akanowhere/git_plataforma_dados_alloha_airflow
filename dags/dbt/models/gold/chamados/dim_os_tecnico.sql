WITH vw_air_tbl_os_tecnico AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_os_tecnico') }}
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
  codigo_deposito,
  posicao_estoque,
  ativo,
  terceirizado,
  id_terceirizada,
  telefone,
  bloqueado_provisionamento_app,
  id_ofs,
  email

FROM vw_air_tbl_os_tecnico
