WITH vw_air_tbl_os AS (
  SELECT
    id,
    data_criacao,
    id_agenda,
    id_chamado,
    agendamento_data,
    turno,
    equipe,
    servico,
    id_tecnico,
    cliente_nome,
    contrato_codigo,
    unidade,
    codigo_fila,
    observacao,
    status,
    {{ translate_column('motivo_conclusao') }} AS motivo_conclusao,
    data_conclusao,
    integracao_codigo,
    posicao_sap

  FROM {{ ref('vw_air_tbl_os') }}
),

vw_air_tbl_contrato AS (
  SELECT
    id,
    data_criacao,
    legado_sistema

  FROM {{ ref('vw_air_tbl_contrato') }}
),

vw_air_tbl_os_tecnico AS (
  SELECT
    id,
    id_terceirizada,
    nome

  FROM {{ ref('vw_air_tbl_os_tecnico') }}
),

vw_air_tbl_os_terceirizada AS (
  SELECT
    id,
    nome

  FROM {{ ref('vw_air_tbl_os_terceirizada') }}
),

vw_air_tbl_catalogo_item AS (
  SELECT
    id_catalogo,
    nome,
    {{ translate_column('codigo') }} AS codigo

  FROM {{ ref('vw_air_tbl_catalogo_item') }}
),

vw_air_tbl_chd_fila AS (
  SELECT
    codigo,
    nome

  FROM {{ ref('vw_air_tbl_chd_fila') }}
),

vw_air_tbl_agenda AS (
  SELECT
    id,
    nome

  FROM {{ ref('vw_air_tbl_agenda') }}
),

vw_air_tbl_chd_chamado AS (
  SELECT
    id,
    data_abertura

  FROM {{ ref('vw_air_tbl_chd_chamado') }}
),

evento_os_finalizada AS (
  SELECT
    usuario,
    id_os,
    tipo

  FROM {{ ref('vw_air_tbl_os_evento') }}

  WHERE tipo = 'EVT_OS_FINALIZADA'

  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_os ORDER BY momento_evento ASC) = 1
),

ult_momento_evento AS (
  SELECT
    id_os,
    tipo,
    momento_evento

  FROM {{ ref('vw_air_tbl_os_evento') }}

  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_os ORDER BY momento_evento DESC) = 1
),

ult_momento_os_iniciada AS (
  SELECT
    id_os,
    momento_evento

  FROM {{ ref('vw_air_tbl_os_evento') }}

  WHERE tipo = 'EVT_OS_INICIADA'

  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_os ORDER BY momento_evento) = 1
),

fato_activity AS (
  SELECT
    TRY_CAST(t.apptnumber AS BIGINT) AS apptnumber,
    TRY_CAST(t.xa_pos_nme_sap AS STRING) AS xa_pos_nme_sap

  FROM {{ ref('fato_activity') }} AS t
  JOIN (
    SELECT
      apptnumber,
      MAX(updatedat) AS max_data

    FROM {{ ref('fato_activity') }}

    WHERE createdat >= '2024-08-01'

    GROUP BY apptnumber
  ) sub ON t.apptnumber = sub.apptnumber AND t.updatedat = sub.max_data

  WHERE t.createdat >= '2024-08-01'
),

joined AS (
  SELECT DISTINCT
    os.id AS codigo_os,
    os.unidade,
    os.contrato_codigo AS codigo_contrato,
    contrato.data_criacao AS data_criacao_contrato,
    os.agendamento_data AS data_agendamento,
    COALESCE(os_terceirizada.nome, 'Sumicity') AS terceirizada,
    os.data_criacao AS data_criacao_os,
    os.turno,
    os.servico,
    os.equipe,
    os_tecnico.nome AS tecnico,
    os.status AS status_os,
    ult_momento_evento.tipo AS status_evento,
    os.id_chamado AS chamado,
    ult_momento_evento.momento_evento AS data_evento,
    catalogo_item.nome AS motivo_conclusao,
    fila.nome AS fila,
    os.cliente_nome AS cliente,
    os.data_conclusao,
    agenda.nome AS agenda,
    CASE
      WHEN evento_os_finalizada.usuario = 'AIR0001' THEN os_tecnico.nome
      ELSE evento_os_finalizada.usuario
    END AS usuario_conclusao,
    CASE
      WHEN os.motivo_conclusao LIKE 'MTV_%' THEN 'Realizado'
      WHEN os.motivo_conclusao LIKE 'NR_%' THEN 'Não Realizado'
      WHEN os.motivo_conclusao LIKE 'CANCEL_%' THEN 'Cancelado'
      WHEN os.motivo_conclusao LIKE 'REAG_%' THEN 'Reagendado'
      WHEN catalogo_item.id_catalogo = 49 THEN 'Cancelado'
      WHEN catalogo_item.id_catalogo = 71 THEN 'Reagendado'
    END AS conclusao,
    os.motivo_conclusao AS motivo_conclusao_bruto,
    ult_momento_os_iniciada.momento_evento AS inicio_servico,
    chamado.data_abertura AS data_abertura_chamado,
    os.data_criacao AS data_abertura_os,
    os.integracao_codigo AS numero_tarefa,
    os.id_tecnico,
    os_tecnico.id_terceirizada,
    os.observacao AS descricao,
    contrato.legado_sistema AS sistema_origem,
    COALESCE(os.posicao_sap, fato_activity.xa_pos_nme_sap) AS posicao_sap

  FROM vw_air_tbl_os AS os
  LEFT JOIN vw_air_tbl_contrato AS contrato ON os.contrato_codigo = contrato.id
  LEFT JOIN vw_air_tbl_os_tecnico AS os_tecnico ON os.id_tecnico = os_tecnico.id
  LEFT JOIN vw_air_tbl_os_terceirizada AS os_terceirizada ON os_tecnico.id_terceirizada = os_terceirizada.id
  LEFT JOIN vw_air_tbl_catalogo_item AS catalogo_item ON os.motivo_conclusao = catalogo_item.codigo
  LEFT JOIN vw_air_tbl_chd_fila AS fila ON os.codigo_fila = fila.codigo
  LEFT JOIN vw_air_tbl_agenda AS agenda ON os.id_agenda = agenda.id
  LEFT JOIN vw_air_tbl_chd_chamado AS chamado ON os.id_chamado = chamado.id
  LEFT JOIN evento_os_finalizada ON os.id = evento_os_finalizada.id_os
  LEFT JOIN ult_momento_evento ON os.id = ult_momento_evento.id_os
  LEFT JOIN ult_momento_os_iniciada ON os.id = ult_momento_os_iniciada.id_os
  LEFT JOIN fato_activity ON os.id = fato_activity.apptnumber
)

SELECT *
FROM joined
