WITH CTE AS (
  SELECT
      id_registro AS id_negociacao,
      fatura_gerada.codigo AS codigo,
      fatura_negociada.codigo AS fatura_original,
      responsavel.id AS id_negociador,
      responsavel.nome AS nm_negociador,
      to_date(data_negociacao, 'dd/MM/yyyy') AS data_negociacao,
      regenociacao AS renegociacao,
      NULL AS fonte_negociacao,
      origem,
      DATEADD(HOUR, -3, current_timestamp()) AS data_extracao
  FROM
      {{ get_catalogo('silver') }}.stage.vw_sydle_tbl_negociacao LATERAL VIEW EXPLODE(
          from_json(
              faturas_geradas,
              'array<struct<codigo:string,id:string>>'
          )
      ) AS fatura_gerada LATERAL VIEW EXPLODE(
          from_json(
              faturas_negociadas,
              'array<struct<codigo:string,id:string>>'
          )
      ) AS fatura_negociada LATERAL VIEW EXPLODE(
          from_json(
              responsavel_negociacao,
              'array<struct<nome:string,id:string>>'
          )
      ) AS responsavel

  UNION ALL

  SELECT
    TRY_CAST(id_negociacao AS STRING),
    TRY_CAST(codigo AS STRING),
    TRY_CAST(fatura_original AS STRING),
    TRY_CAST(id_negociador AS STRING),
    TRY_CAST(nm_negociador AS STRING),
    TRY_CAST(data_criacao AS DATE) AS data_negociacao,
    NULL AS renegociacao,
    TRY_CAST(fonte_negociacao AS STRING),
    NULL AS origem,
    DATEADD(HOUR, -3, current_timestamp()) AS data_extracao
  FROM
    {{ get_catalogo('silver') }}.stage_legado.vw_dim_negociacao_sydle
),

classificacao (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY codigo, fatura_original, data_negociacao ORDER BY CASE WHEN origem is not null then 1 else 2 end ) as rn
  FROM CTE
)

select distinct * from classificacao where rn = 1
