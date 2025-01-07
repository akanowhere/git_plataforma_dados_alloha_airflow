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
      ,ID_EVENTO
      ,momento AS data_evento
      ,motivo_associado
      ,tipo
      ,Vencimento_Antigo
      ,Vencimento_Novo
      ,Fechamento_Antigo
      ,Fechamento_Novo
      ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM base_faturas_do_mes
INNER JOIN {{ ref('faturamento_alteracao_vencimento') }} as faturamento_alteracao_vencimento
	ON base_faturas_do_mes.CODIGO_CONTRATO_AIR = faturamento_alteracao_vencimento.ID_CONTRATO_AIR
	AND YEAR(faturamento_alteracao_vencimento.momento) = YEAR(RIGHT(mes_referencia_emissao, 4))
	AND MONTH(faturamento_alteracao_vencimento.momento) = LEFT(mes_referencia_emissao, 2)

union

SELECT CODIGO_CONTRATO_AIR
      ,ciclo
      ,mes_referencia_emissao
      ,ID_EVENTO
      ,data_evento
      ,motivo_associado
      ,tipo
      ,Vencimento_Antigo
      ,Vencimento_Novo
      ,Fechamento_Antigo
      ,Fechamento_Novo
      ,data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
