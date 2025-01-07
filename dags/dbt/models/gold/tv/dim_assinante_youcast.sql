WITH tbl_assinante_youcast AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_assinante_youcast') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  cliente_id,
  contrato_id,
  cpf_cnpj,
  produto_id,
  ativo,
  email,
  telefone,
  integracao_hub_status,
  integracao_transacao,
  integracao_status,
  integracao_mensagem

FROM tbl_assinante_youcast
