WITH base_faturas_do_mes AS (
  SELECT CODIGO_CONTRATO_AIR
      ,ciclo
      ,mes_referencia_emissao
  FROM {{ ref('base_faturas') }} as base_faturas
  WHERE mes_referencia_emissao =
  CASE
      WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
      THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
      ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
  END
)

SELECT base_faturas_do_mes.CODIGO_CONTRATO_AIR
      ,base_faturas_do_mes.ciclo
      ,base_faturas_do_mes.mes_referencia_emissao
      ,data_processamento AS data_processamento_reajuste
      ,dim_contrato_reajuste.porcentagem_reajuste AS porcentagem_reajustada
      ,valor_anterior
      ,dim_contrato_reajuste.valor_final
      ,versao_contrato
      ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM base_faturas_do_mes
INNER JOIN {{ ref('dim_contrato_reajuste') }} as dim_contrato_reajuste
	ON base_faturas_do_mes.CODIGO_CONTRATO_AIR = dim_contrato_reajuste.id_contrato
	AND YEAR(dim_contrato_reajuste.data_processamento) = YEAR(RIGHT(mes_referencia_emissao, 4))
	AND MONTH(dim_contrato_reajuste.data_processamento) = LEFT(mes_referencia_emissao, 2)

UNION

SELECT CODIGO_CONTRATO_AIR
      ,ciclo
      ,mes_referencia_emissao
      ,data_processamento_reajuste
      ,porcentagem_reajustada
      ,valor_anterior
      ,valor_final
      ,versao_contrato
      ,data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
