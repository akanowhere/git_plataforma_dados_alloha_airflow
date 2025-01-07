WITH air_venda_adicional AS (
    SELECT b.id_contrato, 
    a.data_criacao,
    a.data_alteracao,
    a.id_venda,
    a.produto_codigo,
    a.produto_nome,
    a.valor_total,
    a.quantidade,
    b.excluido
    FROM {{ ref('vw_air_tbl_venda_adicional') }} as a
    LEFT JOIN {{ ref('vw_air_tbl_venda') }} as b ON a.id_venda = b.id
    where a.excluido = FALSE and b.excluido = FALSE
),

base_faturas_do_mes AS (
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
      ,data_criacao AS data_criacao_venda_adicional
      ,id_venda AS id_venda_adicional
      ,produto_codigo
      ,produto_nome
      ,valor_total
      ,quantidade
      ,excluido
      ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM base_faturas_do_mes
INNER JOIN air_venda_adicional
	ON base_faturas_do_mes.CODIGO_CONTRATO_AIR = air_venda_adicional.id_contrato
	AND YEAR(air_venda_adicional.data_criacao) = YEAR(RIGHT(mes_referencia_emissao, 4))
	AND MONTH(air_venda_adicional.data_criacao) = LEFT(mes_referencia_emissao, 2)

UNION

SELECT CODIGO_CONTRATO_AIR
      ,ciclo
      ,mes_referencia_emissao
      ,data_criacao_venda_adicional
      ,id_venda_adicional
      ,produto_codigo
      ,produto_nome
      ,valor_total
      ,quantidade
      ,excluido
      ,data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
