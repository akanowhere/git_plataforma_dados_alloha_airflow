WITH

vw_air_tbl_contrato_retencao AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_contrato_retencao') }}
),

transformed AS (
  SELECT
    id AS id_contrato_retencao,
    DATE_FORMAT(data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
    DATE_FORMAT(data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
    usuario_criacao,
    usuario_alteracao,
    excluido,
    id_contrato,
    status,
    usuario_atribuido,
    motivo_solicitacao,
    motivo_investigado,
    data_reversao_cancelamento,
    desconto_aplicado,
    sub_motivo_cancelamento,
    origem_contato,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM vw_air_tbl_contrato_retencao
)

SELECT *
FROM
  transformed
