WITH base_faturas_do_mes AS (
  SELECT CODIGO_CONTRATO_AIR,
         ciclo,
         mes_referencia_emissao,
         dtCicloFim,
         dtCicloInicio,
         codigo_pacote,
         data_ativacao_contrato,
         data_cancelamento_contrato,
         valor_base,
         valor_final,
         codigo_fatura_sydle,
         classificacao_fatura
  FROM {{ ref('base_faturas') }} as base_faturas
  WHERE mes_referencia_emissao =
  CASE
      WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
      THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
      ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
  END
),

dim_desconto AS (
  SELECT id as id_desconto,
         categoria,
         desconto,
         dia_aplicar,
         data_validade,
         id_contrato,
         item_codigo
  FROM {{ ref('dim_desconto') }} as dim_desconto
  WHERE excluido = FALSE
)

SELECT DISTINCT dim_desconto.id_desconto,
                dim_desconto.categoria,
                dim_desconto.desconto,
                dim_desconto.dia_aplicar,
                dim_desconto.data_validade,
                base_faturas_do_mes.CODIGO_CONTRATO_AIR,
                base_faturas_do_mes.data_ativacao_contrato,
                try_cast(base_faturas_do_mes.data_cancelamento_contrato as string) as data_cancelamento_contrato,
                base_faturas_do_mes.codigo_pacote,
                base_faturas_do_mes.valor_base,
                base_faturas_do_mes.valor_final,
                base_faturas_do_mes.ciclo,
                base_faturas_do_mes.dtCicloInicio,
                base_faturas_do_mes.dtCicloFim,
                base_faturas_do_mes.codigo_fatura_sydle,
                base_faturas_do_mes.classificacao_fatura,
                base_faturas_do_mes.mes_referencia_emissao,
                CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM base_faturas_do_mes
INNER JOIN dim_desconto ON dim_desconto.id_contrato = base_faturas_do_mes.CODIGO_CONTRATO_AIR
AND dim_desconto.dia_aplicar <= base_faturas_do_mes.dtCicloFim
AND dim_desconto.data_validade >= base_faturas_do_mes.dtCicloInicio
AND dim_desconto.item_codigo = base_faturas_do_mes.codigo_pacote

UNION

SELECT id_desconto,
       categoria,
       desconto,
       dia_aplicar,
       data_validade,
       CODIGO_CONTRATO_AIR,
       data_ativacao_contrato,
       data_cancelamento_contrato,
       codigo_pacote,
       valor_base,
       valor_final,
       ciclo,
       dtCicloInicio,
       dtCicloFim,
       codigo_fatura_sydle,
       classificacao_fatura,
       mes_referencia_emissao,
       data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
