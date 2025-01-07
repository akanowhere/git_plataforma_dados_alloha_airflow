WITH base_arvore_com_valor_fatura_somado AS (
	SELECT CODIGO_CONTRATO_AIR,
	SUM(valor_fatura) AS valor_fatura_somado
	FROM {{ ref('base_faturas') }}
	where codigo_fatura_sydle IS NOT NULL
      AND mes_referencia_emissao =
            CASE
            WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
            THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
            ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
            END
	GROUP BY CODIGO_CONTRATO_AIR
)

SELECT A.id_cliente_AIR
      ,A.CODIGO_CONTRATO_AIR
      ,A.codigo_fatura_sydle
      ,A.emissao_fatura
      ,A.vencimento_fatura
      ,A.forma_pagamento_fatura
      ,A.data_pagamento_fatura
      ,A.mes_referencia_fatura
      ,A.mes_referencia_emissao
      ,A.dtCicloInicio
      ,A.dtCicloFim
      ,A.status_contrato
      ,A.nome_pacote
      ,A.codigo_pacote
      ,A.classificacao_fatura
      ,A.status_fatura
      ,A.data_ativacao_contrato
      ,A.data_cancelamento_contrato
      ,A.aging_contrato
      ,A.cidade
      ,A.polo
      ,A.marca
      ,A.UF
      ,A.regiao
      ,A.regional
      ,A.sub_regional
      ,A.valor_base
      ,A.valor_final
      ,base_arvore_com_valor_fatura_somado.valor_fatura_somado AS valor_fatura
      ,A.segmento
      ,A.ciclo
      ,A.dia_vencimento
      ,A.migracao_plano
      ,A.data_migracao
      ,A.tipo_migracao
      ,A.natureza_migracao
      ,A.servicos_adicionais
      ,A.nome_servico_adicional
      ,A.valor_servico_adicional
      ,A.multa
      ,A.tipo_multa
      ,A.dias_base_calculo
      ,A.valor_dia_base_calculo
      ,A.porcentagem_desconto
      ,A.quantidade_desconto
      ,A.valor_desconto
      ,A.dias_suspensos
      ,A.valor_liquido_contrato
      ,A.validacao_API
      ,A.valor_calculo_final_API
      ,tag.tag
      ,tag_isencoes.isencoes
      ,tag_isencoes.tag_isencoes
      ,tag_tela_3.tag_tela_3
      ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM {{ ref('faturamento_base_arvore_inicial') }} as A
LEFT JOIN base_arvore_com_valor_fatura_somado
	ON A.CODIGO_CONTRATO_AIR = base_arvore_com_valor_fatura_somado.CODIGO_CONTRATO_AIR
LEFT JOIN {{ ref('faturamento_tag_isencoes') }} AS tag_isencoes
	ON A.CODIGO_CONTRATO_AIR = tag_isencoes.CODIGO_CONTRATO_AIR
LEFT JOIN {{ ref('faturamento_tag') }} AS tag
	ON A.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
LEFT JOIN {{ ref('faturamento_tag_tela_3') }} AS tag_tela_3
	ON A.CODIGO_CONTRATO_AIR = tag_tela_3.CODIGO_CONTRATO_AIR

union

SELECT id_cliente_AIR
      ,CODIGO_CONTRATO_AIR
      ,codigo_fatura_sydle
      ,emissao_fatura
      ,vencimento_fatura
      ,forma_pagamento_fatura
      ,data_pagamento_fatura
      ,mes_referencia_fatura
      ,mes_referencia_emissao
      ,dtCicloInicio
      ,dtCicloFim
      ,status_contrato
      ,nome_pacote
      ,codigo_pacote
      ,classificacao_fatura
      ,status_fatura
      ,data_ativacao_contrato
      ,data_cancelamento_contrato
      ,aging_contrato
      ,cidade
      ,polo
      ,marca
      ,UF
      ,regiao
      ,regional
      ,sub_regional
      ,valor_base
      ,valor_final
      ,valor_fatura
      ,segmento
      ,ciclo
      ,dia_vencimento
      ,migracao_plano
      ,data_migracao
      ,tipo_migracao
      ,natureza_migracao
      ,servicos_adicionais
      ,nome_servico_adicional
      ,valor_servico_adicional
      ,multa
      ,tipo_multa
      ,dias_base_calculo
      ,valor_dia_base_calculo
      ,porcentagem_desconto
      ,quantidade_desconto
      ,valor_desconto
      ,dias_suspensos
      ,valor_liquido_contrato
      ,validacao_API
      ,valor_calculo_final_API
      ,tag
      ,isencoes
      ,tag_isencoes
      ,tag_tela_3
      ,data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
