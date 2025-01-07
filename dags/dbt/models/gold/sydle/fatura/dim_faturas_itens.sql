WITH silver_stage__vw_sydle_tbl_fatura_itens AS (
  SELECT *
  FROM {{ ref('vw_sydle_tbl_fatura_itens') }}
)

SELECT DISTINCT
      id_fatura
      ,codigo_fatura_sydle
	,CAST(REPLACE(REPLACE(REPLACE(REPLACE((UPPER({{ translate_column(get_catalogo('silver') ~ '.stage_sydle.vw_faturas_itens.nome_item') }})), CHAR(13), ''), CHAR(10), ''), '.', ''), 'º', '') AS VARCHAR(255)) AS nome_item
      ,valor_item
      ,CAST(REPLACE(REPLACE(REPLACE(REPLACE((UPPER({{ translate_column(get_catalogo('silver') ~ '.stage_sydle.vw_faturas_itens.tipo_item') }})), CHAR(13), ''), CHAR(10), ''), '.', ''), 'º', '') AS VARCHAR(255)) AS tipo_item
      ,data_inicio_item
      ,data_fim_item
	,DATEADD(HOUR, -3, current_timestamp()) AS data_extracao

  FROM silver_stage__vw_sydle_tbl_fatura_itens
