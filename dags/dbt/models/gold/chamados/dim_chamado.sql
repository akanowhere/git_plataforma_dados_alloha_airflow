WITH vw_air_tbl_chd_chamado AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_chamado') }}
),

vw_air_tbl_chd_classificacao AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_classificacao') }}
),

vw_air_tbl_chd_fila AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_fila') }}
),

vw_air_tbl_chd_transferencia AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_transferencia') }}
),

vw_air_tbl_chd_transferencia__join__vw_air_tbl_chd_fila AS (
  SELECT
    c.id AS id_transferencia,
    c.data_transferencia,
    id_chamado,
    id_fila_origem,
    fori.nome AS nome_fila_origem

  FROM vw_air_tbl_chd_transferencia AS c
  LEFT JOIN vw_air_tbl_chd_fila AS fdest ON c.id_fila_destino = fdest.id
  LEFT JOIN vw_air_tbl_chd_fila AS fori ON c.id_fila_origem = fori.id
),

vw_air_tbl_vendedor AS (
  SELECT
    id,
    usuario,
    equipe

  FROM {{ ref('vw_air_tbl_vendedor') }}
),

joined AS (
  SELECT
    chamado.id,
    chamado.data_abertura,
    chamado.data_transferencia,
    chamado.data_conclusao,
    chamado.usuario_abertura,
    chamado.usuario_atribuido,
    chamado.motivo_conclusao,
    chamado.codigo_contrato,
    chamado.codigo_cliente,
    chamado.endereco_unidade,
    chamado.endereco_bairro,
    chamado.endereco_logradouro,
    chamado.prioridade,
    chamado.data_prioridade,
    chamado.contrato_b2b,
    classificacao.id AS id_classificacao,
    classificacao.data_alteracao AS data_alteracao_classificacao,
    classificacao.excluido AS excluido_classificacao,
    classificacao.codigo AS codigo_classificacao,
    classificacao.nome AS nome_classificacao,
    classificacao.tipo AS tipo_classificacao,
    classificacao.ativo AS ativo_classificacao,
    fila.id AS id_fila,
    fila.data_alteracao AS data_alteracao_fila,
    fila.excluido AS excluido_fila,
    fila.codigo AS codigo_fila,
    fila.nome AS nome_fila,
    fila.ativo AS ativo_fila,
    fila_transferencia.nome_fila_origem,
    vendedor.equipe AS equipe_origem

  FROM vw_air_tbl_chd_chamado AS chamado
  LEFT JOIN vw_air_tbl_chd_classificacao AS classificacao ON chamado.id_classificacao = classificacao.id
  LEFT JOIN vw_air_tbl_chd_fila AS fila ON chamado.id_fila = fila.id
  LEFT JOIN vw_air_tbl_chd_transferencia__join__vw_air_tbl_chd_fila AS fila_transferencia ON chamado.id = fila_transferencia.id_chamado AND chamado.data_transferencia = fila_transferencia.data_transferencia
  LEFT JOIN vw_air_tbl_vendedor AS vendedor ON chamado.usuario_abertura = vendedor.usuario
)

SELECT *
FROM joined
