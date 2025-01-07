WITH base_faturas_do_mes AS (
  SELECT CODIGO_CONTRATO_AIR ,
       ciclo ,
       dtCicloInicio ,
       dtCicloFim ,
       mes_referencia_emissao
  FROM {{ ref('base_faturas') }} as base_faturas
  WHERE mes_referencia_emissao =
  CASE
      WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
      THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
      ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
  END
)

SELECT base_faturas_do_mes.CODIGO_CONTRATO_AIR ,
       dim_migracao.id AS id_migracao ,
       dim_migracao.data_venda AS data_migracao ,
       base_faturas_do_mes.ciclo ,
       base_faturas_do_mes.dtCicloInicio ,
       base_faturas_do_mes.dtCicloFim ,
       base_faturas_do_mes.mes_referencia_emissao ,
       dim_migracao.antigo_pacote_codigo ,
       dim_migracao.antigo_valor ,
       dim_migracao.novo_pacote_codigo ,
       dim_migracao.novo_valor ,
       CASE
           WHEN dim_migracao.antigo_valor < dim_migracao.novo_valor THEN 'Upgrade'
           WHEN dim_migracao.antigo_valor > dim_migracao.novo_valor THEN 'Dowgrade'
           WHEN dim_migracao.antigo_valor = dim_migracao.novo_valor THEN 'Manteve'
           ELSE NULL
       END AS tipo_migracao ,
       dim_migracao.id_venda ,
       air_vendas_migracao.natureza AS natureza_venda ,
       dim_campanha.id_campanha ,
       dim_campanha.nome AS nome_campanha ,
       CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM base_faturas_do_mes
INNER JOIN {{ ref('dim_migracao') }} as dim_migracao ON base_faturas_do_mes.CODIGO_CONTRATO_AIR = dim_migracao.id_contrato
											AND YEAR(dim_migracao.data_venda) = YEAR(RIGHT(mes_referencia_emissao, 4))
											AND MONTH(dim_migracao.data_venda) = LEFT(mes_referencia_emissao, 2)
LEFT JOIN {{ ref('faturamento_air_vendas_migracao') }} as air_vendas_migracao ON air_vendas_migracao.id = dim_migracao.id_venda
LEFT JOIN {{ ref('dim_campanha') }} as dim_campanha ON dim_campanha.id_campanha = air_vendas_migracao.id_campanha

UNION

SELECT CODIGO_CONTRATO_AIR ,
       id_migracao ,
       data_migracao ,
       ciclo ,
       dtCicloInicio ,
       dtCicloFim ,
       mes_referencia_emissao ,
       antigo_pacote_codigo ,
       antigo_valor ,
       novo_pacote_codigo ,
       novo_valor ,
       tipo_migracao ,
       id_venda ,
       natureza_venda ,
       id_campanha ,
       nome_campanha ,
       data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
