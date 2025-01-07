with ult_contrato_b2b as (
SELECT
    MAX(id) AS id, id_cliente
  FROM {{ ref('vw_air_tbl_contrato') }} AS c   
  WHERE b2b IS TRUE
    AND ( legado_sistema NOT IN (
        'ixc_click',
        'integrator_univox',
        'ixc_univox',
        'protheus_ligue',
        'ng_vip',
        'mk_niu',
        'ng_niu',
        'ng_pamnet',
        'mk_pamnet',
        'adapter',
        'URBE'
      ) OR legado_sistema IS NULL)
    AND (data_criacao >= '2023-01-01' OR status <> 'ST_CONT_CANCELADO'
    ) GROUP BY id_cliente
),

ult_contrato_pme as (
SELECT
    MAX(id) AS id, id_cliente
  FROM {{ ref('vw_air_tbl_contrato') }} AS c
  WHERE pme IS TRUE
    AND ( legado_sistema NOT IN (
        'ixc_click',
        'integrator_univox',
        'ixc_univox',
        'protheus_ligue',
        'ng_vip',
        'mk_niu',
        'ng_niu',
        'ng_pamnet',
        'mk_pamnet',
        'adapter',
        'URBE')
      OR legado_sistema IS NULL)
    AND ( data_criacao >= '2023-01-01' OR status <> 'ST_CONT_CANCELADO'
    ) GROUP BY id_cliente
)



SELECT
  CAST(c.id AS STRING) AS id_cliente,
  DATE_FORMAT(c.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
  DATE_FORMAT(c.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
  CAST(c.excluido AS STRING) AS excluido,
  CAST(c.tipo AS STRING) AS tipo,
  CAST(c.nome AS STRING) AS nome,
  CAST(c.grupo AS STRING) AS grupo,
  CASE
    WHEN contratob2b.id IS NOT NULL THEN 'B2B'
    WHEN contratopme.id IS NOT NULL THEN 'PME'
    WHEN contratob2b.id IS NULL
      AND contratopme.id IS NULL THEN 'B2C'
  END AS segmento,
  CAST(c.legado_id AS STRING) AS legado_id,
  CAST(c.legado_sistema AS STRING) AS legado_sistema,
  CAST(c.integracao_status AS STRING) AS integracao_status,
  CAST(c.integracao_codigo AS STRING) AS integracao_codigo,
  CAST(c.marcador AS STRING) AS marcador,
  CAST(c.integracao_status_sydle AS STRING) AS integracao_status_sydle,
  CAST(cf.cpf AS STRING) AS cpf,
  DATE_FORMAT(cf.data_nascimento, 'yyyy-MM-dd HH:mm:ss') AS data_nascimento,
  CAST(cf.rg_numero AS STRING) AS rg_numero,
  CAST(cf.rg_orgao AS STRING) AS rg_orgao,
  CAST(cf.estado_civil AS STRING) AS estado_civil,
  CAST(cf.nome_pai AS STRING) AS nome_pai,
  CAST(cf.nome_mae AS STRING) AS nome_mae,
  CAST(cj.cnpj AS STRING) AS cnpj,
  CAST(cj.inscricao_estadual AS STRING) AS inscricao_estadual,
  CAST(cj.inscricao_municipal AS STRING) AS inscricao_municipal,
  CAST(cj.nome_fantasia AS STRING) AS nome_fantasia,
  CAST(cj.atividade_principal AS STRING) AS atividade_principal,
  'AIR' AS fonte,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
  CAST(c.cupom AS STRING) AS cupom,
  CAST(c.fonte AS STRING) AS fonte_air
  
FROM {{ ref('vw_air_tbl_cliente') }} AS c 
LEFT JOIN {{ ref('vw_air_tbl_cliente_fisico') }} AS cf ON c.id = cf.id_cliente
LEFT JOIN {{ ref('vw_air_tbl_cliente_juridico') }} AS cj ON c.id = cj.id_cliente
LEFT JOIN  ult_contrato_b2b as contratob2b ON c.id = contratob2b.id_cliente
LEFT JOIN  ult_contrato_pme as contratopme ON c.id = contratopme.id_cliente
