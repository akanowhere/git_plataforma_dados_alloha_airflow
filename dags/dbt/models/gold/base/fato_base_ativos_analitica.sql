{{
    config(
        materialized='incremental'
    )
}}


SELECT
  data_referencia,
  base.id_contrato,
  UPPER(base.cidade) AS cidade,
  UPPER(base.estado) AS estado,
  UPPER(base.canal) AS canal,
  CASE
    WHEN base.legado_sistema = 'ixc_click' THEN 'CLICK'
    ELSE UPPER(base.marca)
  END AS marca,
  UPPER(base.unidade_atendimento) AS unidade,
  CASE
    WHEN UPPER(base.regional) LIKE 'REGIAO_0%'
      OR UPPER(
        base.regional
      ) LIKE 'REGIAO-0%'
      THEN REPLACE(
          REPLACE(UPPER(base.regional), 'EGIAO_0', ''),
          'EGIAO-0',
          ''
        )
    ELSE UPPER(base.regional)
  END AS regional,
  UPPER(base.produto) AS produto,
  UPPER(base.pacote_tv) AS pacote_tv,
  tipo_tv,
  dia_vencimento,
  dia_fechamento,
  segmento,
  CASE
    WHEN aging < 90 THEN 'Abaixo 3 meses'
    WHEN aging >= 90
      AND aging < 180 THEN 'Entre 3 e 6 meses'
    WHEN aging >= 180
      AND aging < 270 THEN 'Entre 6 e 9 meses'
    WHEN aging >= 270
      AND aging < 365 THEN 'Entre 9 e 12 meses'
    WHEN aging >= 365
      AND aging < 545 THEN 'Entre 13 e 18 meses'
    WHEN aging >= 545
      AND aging < 720 THEN 'Entre 19 e 24 meses'
    WHEN aging >= 720 THEN 'Acima de 24 meses'
    ELSE ''
  END AS aging_meses_cat,
  equipe,
  cluster,
  fonte,
  legado_sistema,
  canal_tratado,
  tipo_canal,
  macro_regional,
  nome_regional
FROM
  (
    SELECT DISTINCT
      DATE_SUB(CURRENT_DATE(), 1) AS data_referencia,
      id_contrato,
      status,
      data_ativacao,
      data_cancelamento,
      motivo_cancelamento,
      data_cadastro_sistema,
      x.cidade,
      regiao,
      estado,
      canal,
      x.marca,
      data_extracao,
      unidade_atendimento,
      COALESCE(
        y.sigla_regional,
        x.regional
      ) AS regional,
      produto,
      pacote_tv,
      segmento,
      dia_fechamento,
      dia_vencimento,
      tipo_cancelamento,
      cancelamento_invol,
      equipe,
      sub_motivo_cancelamento,
      tipo_tv,
      DATEDIFF(
        DATE_SUB(CAST(CURRENT_DATE() AS DATE), 1) ,
        date(data_ativacao)
      ) AS aging,
      REPLACE(
        COALESCE(
          y.subregional,
          x.cluster
        ),
        'CLUSTER_',
        ''
      ) AS cluster,
      fonte,
      legado_sistema,
      CASE
        WHEN canal = 'TLV RECEPTIVO' THEN 'RECEPTIVO'
        WHEN aa.contract_air_id IS NOT NULL THEN 'GIGA EMBAIXADOR'
        WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 'GIGA EMBAIXADOR'
        ELSE canal
      END AS canal_tratado,
      CASE
        WHEN aa.contract_air_id IS NOT NULL THEN 'TERCEIRIZADO'
        WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 'TERCEIRIZADO'
        ELSE tipo_canal
      END AS tipo_canal,
      y.macro_regional,
      y.regional1 AS nome_regional
    FROM {{ ref('vw_situacao_contrato') }} AS x
    LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais AS y
      ON (REPLACE(REPLACE(UPPER({{ translate_column('x.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(y.cidade_sem_acento)
          OR REPLACE(REPLACE(UPPER({{ translate_column('x.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(y.cidade))
      AND UPPER(x.estado) = UPPER(y.uf)
    LEFT JOIN {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_consultor AS aa ON aa.contract_air_id = id_contrato
    WHERE
      CAST(data_ativacao AS DATE) <= DATE_SUB(CURRENT_DATE(), 1)
      AND (
        data_cancelamento IS NULL
        OR CAST(data_cancelamento AS DATE) >= CAST(CURRENT_DATE() AS DATE)
      )
      AND status IN (
        'Habilitado',
        'Suspenso p/ Cancelamento',
        'Suspenso p/ Débito',
        'Suspenso p/ Solicitação'
      )
  ) AS base

{% if is_incremental() %}

  WHERE data_referencia > coalesce((select max(data_referencia) from {{ this }}), '1900-01-01')
 

{% endif %}