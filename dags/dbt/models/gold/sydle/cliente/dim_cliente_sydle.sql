WITH cte AS (
  SELECT
    id_cliente,
    ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_integracao ASC) AS num
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS cliente
),
filtereddata AS (
  SELECT *
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS cliente
  WHERE
    id_cliente IN (SELECT id_cliente FROM cte WHERE num = 1)
)

SELECT DISTINCT
  cliente.id_cliente,
  nome,
  razao_social,
  email_principal,
  data_nascimento,
  estado_civil,
  nacionalidade,
  naturalidade,
  falecido,
  sigla,
  codigo_externo,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM filtereddata AS cliente
