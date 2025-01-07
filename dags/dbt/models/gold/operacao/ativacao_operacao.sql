WITH momento_ativacao AS (

  SELECT
    momento,
    id_contrato,
    ROW_NUMBER() OVER (PARTITION BY id_contrato ORDER BY id_contrato) AS is_first
  FROM {{ ref('vw_air_tbl_cliente_evento') }}
  WHERE tipo = 'EVT_CONTRATO_SERVICOS_ATIVADOS'
),

os_ativacao AS (
  SELECT
    try_cast(id as string) AS codigo_os,
    contrato_codigo,
    data_conclusao,
    id_tecnico,
    id_chamado AS codigo_chamado,
    posicao_sap,
    ROW_NUMBER() OVER (PARTITION BY contrato_codigo ORDER BY id DESC) AS is_last,
    motivo_conclusao
  FROM {{ ref('vw_air_tbl_os') }}
  WHERE servico LIKE 'ATIVAÇÃO'
    AND motivo_conclusao LIKE 'MTV_%'
),

OFS as (
  SELECT DISTINCT
    try_cast(t.apptNumber as bigint) as apptNumber,
    try_cast(t.XA_POS_NME_SAP as string) as XA_POS_NME_SAP
  FROM {{ ref('fato_activity') }} t
    JOIN (
      SELECT apptNumber, MAX(updatedAt) AS max_data
      FROM {{ ref('fato_activity') }}
      where createdAt >= '2023-01-01' --'2024-08-01'
      GROUP BY apptNumber
    ) sub ON t.apptNumber = sub.apptNumber AND t.updatedAt = sub.max_data
  where t.createdAt >= '2023-01-01' --'2024-08-01'
  and t.XA_POS_NME_SAP IS NOT NULL
),

fato_ativacao AS (
  SELECT
    contrato.id AS codigo_contrato,
    contrato.status AS status_contrato,
    UPPER(cliente.nome) AS nome_cliente,
    DATE(contrato.data_criacao) AS data_criacao,
    DATE(contrato.data_primeira_ativacao) AS data_primeira_ativacao,
    DATE(ma.momento) AS data_ativacao_evento,
    COALESCE(DATE_FORMAT(momento, "HH:MM:ss"), 'Não Ativo') AS hora_ativacao,
    contrato.unidade_atendimento AS unidade,
    tecnico.nome AS tecnico,
    COALESCE(terceirizada.nome, 'SUMICITY') AS terceirizada,
    contrato.legado_sistema AS sistema_origem,
    os.codigo_os,
    os.codigo_chamado,
    DATE(os.data_conclusao) AS data_conclusao_os,
    DATE_FORMAT(os.data_conclusao, "HH:MM:ss") AS hora_conclusao_os,
    --coalesce(os.posicao_sap , OFS.XA_POS_NME_SAP) AS posicao_sap
    case
        when (os.posicao_sap is null or trim(os.posicao_sap) = '' ) then OFS.XA_POS_NME_SAP
        else os.posicao_sap
    end as posicao_sap

  FROM {{ ref('vw_air_tbl_contrato') }} AS contrato
  LEFT JOIN momento_ativacao AS ma
    ON contrato.id = ma.id_contrato AND is_first = 1
  INNER JOIN {{ ref('vw_air_tbl_cliente') }} AS cliente
    ON contrato.id_cliente = cliente.id
  LEFT JOIN os_ativacao AS os
    ON contrato.id = os.contrato_codigo AND is_last = 1
  LEFT JOIN {{ ref('vw_air_tbl_os_tecnico') }} AS tecnico
    ON os.id_tecnico = tecnico.id
  LEFT JOIN {{ ref('vw_air_tbl_os_terceirizada') }} AS terceirizada
    ON tecnico.id_terceirizada = terceirizada.id
  LEFT JOIN OFS as OFS
    ON OFS.apptNumber = os.codigo_os

  WHERE contrato.data_criacao >= '2023-01-01'
  ORDER BY 1 DESC
)

SELECT distinct *
FROM fato_ativacao

