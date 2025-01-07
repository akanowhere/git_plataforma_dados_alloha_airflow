{{ config(
    materialized='table',
    liquid_clustered_by=['id_cliente_AIR', 'mes_referencia_emissao']
) }}

WITH endereco_contrato AS (
    SELECT E.estado,
        E.cidade,
        E.unidade,
        A.id_contrato_air AS CODIGO_CONTRATO_AIR
    FROM {{ ref('dim_contrato') }} A
    INNER JOIN
        (SELECT MAX(id_endereco) AS endereco,
                id_contrato
        FROM {{ ref('dim_contrato_entrega') }}
        GROUP BY id_contrato) AS tb_temp ON tb_temp.id_contrato = A.id_contrato_air
    INNER JOIN {{ ref('dim_endereco') }} E ON E.id_endereco = tb_temp.endereco
),

base_faturas_com_regras AS (
    SELECT DISTINCT C.contrato_air,
    C.id_cliente_air,
    C.codigo_fatura_sydle,
    C.data_criacao AS emissao_fatura,
    C.data_vencimento AS vencimento_fatura,
    C.forma_pagamento AS forma_pagamento_fatura,
    C.data_pagamento AS data_pagamento_fatura,
    C.mes_referencia AS mes_referencia_fatura,
    C.classificacao AS classificacao_fatura,
    C.status_fatura,
    C.valor_fatura
    FROM {{ ref('dim_faturas_mailing') }} AS C
    WHERE UPPER({{ translate_column('C.classificacao') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
    AND MONTH(C.data_criacao) = MONTH(DATEADD(DAY, -1, current_date()))
    AND YEAR(C.data_criacao) = YEAR(DATEADD(DAY, -1, current_date()))
    AND UPPER({{ translate_column('C.status_fatura') }}) IN ('BAIXA OPERACIONAL', 'EMITIDA', 'PAGA', 'NEGOCIADA COM O CLIENTE')
    AND ((C.mes_referencia = CASE
                                WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
                                THEN '0' || CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
                                ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
                            END)
        OR (C.mes_referencia = CASE
                                    WHEN LENGTH(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6
                                    THEN '0' || CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
                                    ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
                                END))
),

ref_atual_tbl_basao AS (
    SELECT data_referencia,
    codigo_contrato_air,
    codigo_cliente,
    unidade,
    nome_pacote,
    codigo_pacote,
    status_contrato,
    data_ativacao_contrato,
    ticket_final,
    cidade,
    marca,
    uf,
    segmento
    FROM {{ ref('tbl_basao') }}
    WHERE
    CASE
        WHEN LENGTH(CONCAT(MONTH(data_referencia), '/', YEAR(data_referencia))) = 6
        THEN CONCAT('0', CONCAT(MONTH(data_referencia), '/', YEAR(data_referencia)))
        ELSE CONCAT(MONTH(data_referencia), '/', YEAR(data_referencia))
    END
    =
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END --AS mes_referencia_emissao
    AND UPPER(status_contrato) in ('HABILITADO','SUSP_SOLICITACAO','SUSP_DEBITO','SUSP_CANCELAMENTO')
    AND data_ativacao_contrato IS NOT NULL
    AND flag_cnpj_sumicity = 0
),

base_faturamento_basao as (
       SELECT DISTINCT COALESCE(vw_dim_cliente.id_cliente, B.codigo_cliente) AS id_cliente_AIR,
                    B.codigo_contrato_air AS CODIGO_CONTRATO_AIR,
                    C.codigo_fatura_sydle,
                    C.emissao_fatura,
                    C.vencimento_fatura,
                    C.forma_pagamento_fatura,
                    C.data_pagamento_fatura,
                    C.mes_referencia_fatura,
                    CASE
                        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
                        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
                        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
                    END AS mes_referencia_emissao,
                    CASE
                        WHEN A.fechamento_vencimento is null THEN NULL
                        ELSE DATE(CONCAT(Year(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '-', MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '-', A.fechamento_vencimento)) END AS dtCicloInicio,
                    CASE
                        WHEN A.fechamento_vencimento is null THEN NULL
                        ELSE DATE(DATEADD(DAY, -1, DATE(CONCAT(YEAR(DATEADD(DAY, -1, current_date())), '-', MONTH(DATEADD(DAY, -1, current_date())), '-', A.fechamento_vencimento)))) END AS dtCicloFim,
                    B.status_contrato,
                    B.nome_pacote,
                    B.codigo_pacote,
                    C.classificacao_fatura,
                    C.status_fatura,
                    B.data_ativacao_contrato,
                    A.data_cancelamento AS data_cancelamento_contrato,
                    DATEDIFF(DAY, B.data_ativacao_contrato, current_date()) AS aging_contrato,
                    CAST(REPLACE(REPLACE(UPPER({{ translate_column('B.cidade') }}), CHAR(13), ''), CHAR(10), '') AS VARCHAR(255)) as cidade,
                    CASE
                        WHEN UPPER(B.marca) like 'SUMICITY' THEN 'Polo Sumicity'
                        WHEN UPPER(B.marca) like 'CLICK' THEN 'Polo Sumicity'
                        WHEN UPPER(B.marca) like '%GIGA%' THEN 'Polo Sumicity'
                        WHEN UPPER(B.marca) like '%UNIVOX%' THEN 'Polo Sumicity'
                        WHEN UPPER(B.marca) like '%VIP%' THEN 'Polo VIP'
                        WHEN UPPER(B.marca) like '%NIU%' THEN 'Polo VIP'
                        WHEN UPPER(B.marca) like '%PAMNET%' THEN 'Polo VIP'
                        WHEN UPPER(B.marca) like '%LIGUE%' THEN 'Polo VIP'
                        WHEN UPPER(B.marca) like '%MOB%' THEN 'Polo Mob'
                    ELSE NULL
                    END AS polo,
                    B.marca,
                    B.uf AS UF,
                    UPPER(subregionais.macro_regional) AS Regiao,
                    UPPER(subregionais.sigla_regional) AS regional,
                    UPPER(subregionais.subregional) AS sub_regional,
                    A.valor_base,
                    B.ticket_final as valor_final,
                    C.valor_fatura,
                    B.segmento,
                    A.fechamento_vencimento AS ciclo,
                    A.dia_vencimento
    FROM ref_atual_tbl_basao as B
    LEFT JOIN {{ ref('dim_contrato') }} A on B.codigo_contrato_air = A.id_contrato_air
    LEFT JOIN base_faturas_com_regras C ON B.codigo_contrato_air = C.contrato_air
    LEFT JOIN {{ ref('dim_cliente') }} AS vw_dim_cliente ON COALESCE(C.id_cliente_air, B.codigo_cliente) = vw_dim_cliente.id_cliente
        AND vw_dim_cliente.fonte = 'AIR'
    LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais as subregionais
    ON (REPLACE(REPLACE(UPPER({{ translate_column('B.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(subregionais.cidade_sem_acento)
            OR REPLACE(REPLACE(UPPER({{ translate_column('B.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(subregionais.cidade))
        AND UPPER(B.uf) = UPPER(subregionais.uf)
),

base_faturamento_cancelados as (
       SELECT DISTINCT COALESCE(vw_dim_cliente.id_cliente, A.id_cliente) AS id_cliente_AIR,
                    A.id_contrato_air AS CODIGO_CONTRATO_AIR,
                    C.codigo_fatura_sydle,
                    C.emissao_fatura,
                    C.vencimento_fatura,
                    C.forma_pagamento_fatura,
                    C.data_pagamento_fatura,
                    C.mes_referencia_fatura,
                    CASE
                        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
                        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
                        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
                    END AS mes_referencia_emissao,
                    CASE
                        WHEN A.fechamento_vencimento is null THEN NULL
                        ELSE DATE(CONCAT(Year(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '-', MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '-', A.fechamento_vencimento)) END AS dtCicloInicio,
                    CASE
                        WHEN A.fechamento_vencimento is null THEN NULL
                        ELSE DATE(DATEADD(DAY, -1, DATE(CONCAT(YEAR(DATEADD(DAY, -1, current_date())), '-', MONTH(DATEADD(DAY, -1, current_date())), '-', A.fechamento_vencimento)))) END AS dtCicloFim,
                    A.status AS status_contrato,
                    A.pacote_nome AS nome_pacote,
                    A.pacote_codigo AS codigo_pacote,
                    C.classificacao_fatura,
                    C.status_fatura,
                    A.data_primeira_ativacao AS data_ativacao_contrato,
                    A.data_cancelamento AS data_cancelamento_contrato,
                    DATEDIFF(DAY, A.data_primeira_ativacao, current_date()) AS aging_contrato,
                    CAST(REPLACE(REPLACE(UPPER({{ translate_column('endereco_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') AS VARCHAR(255)) as cidade,
                    CASE
                        WHEN UPPER(U.marca) like 'SUMICITY' THEN 'Polo Sumicity'
                        WHEN UPPER(U.marca) like 'CLICK' THEN 'Polo Sumicity'
                        WHEN UPPER(U.marca) like '%GIGA%' THEN 'Polo Sumicity'
                        WHEN UPPER(U.marca) like '%UNIVOX%' THEN 'Polo Sumicity'
                        WHEN UPPER(U.marca) like '%VIP%' THEN 'Polo VIP'
                        WHEN UPPER(U.marca) like '%NIU%' THEN 'Polo VIP'
                        WHEN UPPER(U.marca) like '%PAMNET%' THEN 'Polo VIP'
                        WHEN UPPER(U.marca) like '%LIGUE%' THEN 'Polo VIP'
                        WHEN UPPER(U.marca) like '%MOB%' THEN 'Polo Mob'
                    ELSE NULL
                    END AS polo,
                    U.marca,
                    endereco_contrato.estado AS UF,
                    UPPER(subregionais.macro_regional) AS Regiao,
                    UPPER(subregionais.sigla_regional) AS regional,
                    UPPER(subregionais.subregional) AS sub_regional,
                    A.valor_base,
                    A.valor_final,
                    C.valor_fatura,
                    CASE
                        WHEN A.b2b = 'true' THEN 'B2B'
                        WHEN A.pme = 'true' THEN 'PME'
                        ELSE 'B2C'
                    END AS segmento,
                    A.fechamento_vencimento AS ciclo,
                    A.dia_vencimento
    FROM {{ ref('dim_contrato') }} A
    LEFT JOIN base_faturas_com_regras C ON A.id_contrato_air = C.contrato_air
    LEFT JOIN {{ ref('dim_cliente') }} AS vw_dim_cliente ON COALESCE(C.id_cliente_air, A.id_cliente) = vw_dim_cliente.id_cliente
        AND vw_dim_cliente.fonte = 'AIR'
    LEFT JOIN endereco_contrato ON endereco_contrato.CODIGO_CONTRATO_AIR = A.id_contrato_air
    LEFT JOIN {{ ref('dim_unidade') }} as U ON U.sigla = COALESCE(endereco_contrato.unidade, A.unidade_atendimento)
        AND U.fonte = 'AIR'
    LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais as subregionais
    ON (REPLACE(REPLACE(UPPER({{ translate_column('endereco_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(subregionais.cidade_sem_acento)
            OR REPLACE(REPLACE(UPPER({{ translate_column('endereco_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(subregionais.cidade))
        AND UPPER(endereco_contrato.estado) = UPPER(subregionais.uf)
    WHERE UPPER(A.status) = 'ST_CONT_CANCELADO'
    AND MONTH(A.data_cancelamento) = MONTH(DATEADD(DAY, -1, current_date()))
    AND YEAR(A.data_cancelamento) = YEAR(DATEADD(DAY, -1, current_date()))
),

uniao_basao_e_cancelados AS (
  select DISTINCT *
  FROM base_faturamento_basao

  UNION

  select DISTINCT *
  FROM base_faturamento_cancelados
),

migracao as (
    SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    CAST(dim_migracao.data_venda AS DATE) AS data_migracao,
    air_vendas_migracao.natureza AS natureza_migracao,
    CASE
        WHEN dim_migracao.data_venda IS NOT NULL THEN 1
        ELSE 0
    END AS migracao_plano,
    CASE
        WHEN dim_migracao.antigo_valor < dim_migracao.novo_valor THEN 'Upgrade'
        WHEN dim_migracao.antigo_valor > dim_migracao.novo_valor THEN 'Dowgrade'
        WHEN dim_migracao.antigo_valor = dim_migracao.novo_valor THEN 'Manteve'
        ELSE NULL
    END AS tipo_migracao

    FROM uniao_basao_e_cancelados
    LEFT JOIN
    (SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
            MAX(dim_migracao.id_venda) id_venda_max
    FROM uniao_basao_e_cancelados
    INNER JOIN {{ ref('dim_migracao') }} as dim_migracao ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = dim_migracao.id_contrato
    AND dim_migracao.data_venda BETWEEN dtCicloInicio AND dtCicloFim
    GROUP BY uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR)
    tb_ult_migracao ON tb_ult_migracao.CODIGO_CONTRATO_AIR = uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR
    INNER JOIN {{ ref('dim_migracao') }} as dim_migracao ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = dim_migracao.id_contrato
                                            AND dim_migracao.id_venda = tb_ult_migracao.id_venda_max
    LEFT JOIN {{ ref('faturamento_air_vendas_migracao') }} as air_vendas_migracao ON air_vendas_migracao.id = tb_ult_migracao.id_venda_max
    WHERE mes_referencia_emissao =
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END
),

dias_base_calculo AS (
    SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    (DATEDIFF(DAY, dtCicloInicio, dtCicloFim) + 1) AS dias_base_calculo,
    ((coalesce(valor_final, valor_base)) / (DATEDIFF(DAY, dtCicloInicio, dtCicloFim) + 1)) AS valor_dia_base_calculo
    FROM uniao_basao_e_cancelados
),

base_descontos AS (
    SELECT DISTINCT dim_desconto.id as id_desconto ,
                    dim_desconto.categoria ,
                    dim_desconto.desconto AS porcentagem_desconto ,
                    dim_desconto.dia_aplicar ,
                    dim_desconto.data_validade ,
                    uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR ,
                    uniao_basao_e_cancelados.data_ativacao_contrato ,
                    uniao_basao_e_cancelados.data_cancelamento_contrato ,
                    uniao_basao_e_cancelados.codigo_pacote ,
                    uniao_basao_e_cancelados.valor_base ,
                    uniao_basao_e_cancelados.valor_final ,
                    uniao_basao_e_cancelados.ciclo ,
                    uniao_basao_e_cancelados.dtCicloInicio ,
                    uniao_basao_e_cancelados.dtCicloFim ,
                    uniao_basao_e_cancelados.codigo_fatura_sydle ,
                    uniao_basao_e_cancelados.classificacao_fatura ,
                    uniao_basao_e_cancelados.mes_referencia_emissao ,
                    DATEADD(HOUR, -3, getdate()) AS data_extracao
    FROM uniao_basao_e_cancelados
    INNER JOIN {{ ref('dim_desconto') }} as dim_desconto ON dim_desconto.id_contrato = uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR
    AND dim_desconto.excluido = '0'
    AND dim_desconto.dia_aplicar <= uniao_basao_e_cancelados.dtCicloFim
    AND dim_desconto.data_validade >= uniao_basao_e_cancelados.dtCicloInicio
    AND dim_desconto.item_codigo = uniao_basao_e_cancelados.codigo_pacote
    WHERE mes_referencia_emissao =
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END
),

desconto_somado AS (
    SELECT SUM(porcentagem_desconto) AS porcentagem,
            count(id_desconto) AS quantidade_desconto,
            CODIGO_CONTRATO_AIR,
            mes_referencia_emissao,
            codigo_fatura_sydle
    FROM base_descontos
    GROUP BY CODIGO_CONTRATO_AIR,
            mes_referencia_emissao,
            codigo_fatura_sydle
),

dados_desconto AS (
    SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR ,
    desconto_somado.porcentagem AS porcentagem_desconto,
    desconto_somado.quantidade_desconto AS quantidade_desconto,
    COALESCE(((COALESCE(valor_final, valor_base) * desconto_somado.porcentagem)/100), 0) AS valor_desconto
    FROM uniao_basao_e_cancelados
    INNER JOIN desconto_somado ON desconto_somado.CODIGO_CONTRATO_AIR = uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR
        AND desconto_somado.mes_referencia_emissao = uniao_basao_e_cancelados.mes_referencia_emissao
        AND desconto_somado.codigo_fatura_sydle = uniao_basao_e_cancelados.codigo_fatura_sydle
),

--------------------------------------
---- DIAS SUSPENSOS
--------------------------------------

suspensao_e_ativacao_dentro_do_ciclo AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE))) IS NULL THEN 0
	 ELSE SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE))) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtAtivacao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtSuspensao AS DATE) >= dtCicloInicio AND
		CAST(suspensao.dtAtivacao AS DATE) <= dtCicloFim
	WHERE DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE)) <> 0
	GROUP BY A.CODIGO_CONTRATO_AIR
),

comecou_suspenso_e_ativou_dentro_do_ciclo AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM((DATEDIFF(day,dtCicloInicio,CAST(suspensao.dtAtivacao AS DATE)))) IS NULL THEN 0
	 ELSE SUM((DATEDIFF(day,dtCicloInicio,CAST(suspensao.dtAtivacao AS DATE)))) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtAtivacao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtSuspensao AS DATE) < dtCicloInicio AND
		CAST(suspensao.dtAtivacao AS DATE) >= dtCicloInicio AND
		CAST(suspensao.dtAtivacao AS DATE) <= dtCicloFim
	WHERE DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE)) <> 0
	GROUP BY A.CODIGO_CONTRATO_AIR
),

suspenso_antes_do_ciclo_e_ativou_apos_ciclo AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM(DATEDIFF(DAY,dtCicloInicio, dtCicloFim )+1) IS NULL THEN 0
	 ELSE SUM(DATEDIFF(DAY,dtCicloInicio, dtCicloFim )+1) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtAtivacao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtSuspensao AS DATE) < dtCicloInicio  AND
		CAST(suspensao.dtAtivacao AS DATE) > dtCicloFim
	WHERE DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE)) <> 0
	GROUP BY A.CODIGO_CONTRATO_AIR
),

suspenso_dentro_do_ciclo_e_ativou_apos_ciclo AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),dtCicloFim)+1) IS NULL THEN 0
	 ELSE SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),dtCicloFim)+1) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtAtivacao AS DATE) IS NOT NULL AND
		CAST(suspensao.dtSuspensao AS DATE) >= dtCicloInicio  AND
		CAST(suspensao.dtSuspensao AS DATE) <= dtCicloFim AND
		CAST(suspensao.dtAtivacao AS DATE) > dtCicloFim
	WHERE DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),CAST(suspensao.dtAtivacao AS DATE)) <> 0
	GROUP BY A.CODIGO_CONTRATO_AIR
),

suspenso_dentro_do_ciclo_e_ainda_esta_suspenso AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),dtCicloFim)+1) IS NULL THEN 0
	 ELSE SUM(DATEDIFF(DAY,CAST(suspensao.dtSuspensao AS DATE),dtCicloFim)+1) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) IS NOT NULL AND
		suspensao.ID_EVENTO_ATIVACAO IS NULL AND
		CAST(suspensao.dtSuspensao AS DATE) >= dtCicloInicio  AND
		CAST(suspensao.dtSuspensao AS DATE) <= dtCicloFim
	GROUP BY A.CODIGO_CONTRATO_AIR
),

suspenso_antes_do_ciclo_e_ainda_esta_suspenso AS (
	SELECT DISTINCT
	A.CODIGO_CONTRATO_AIR,
	CASE
	 WHEN SUM(DATEDIFF(DAY,dtCicloInicio ,dtCicloFim)+1) IS NULL THEN 0
	 ELSE SUM(DATEDIFF(DAY,dtCicloInicio ,dtCicloFim)+1) END AS Dias
	FROM uniao_basao_e_cancelados A
	INNER JOIN {{ ref('faturamento_suspensao') }} as suspensao
		ON A.CODIGO_CONTRATO_AIR  = suspensao.ID_CONTRATO_AIR AND
		CAST(suspensao.dtSuspensao AS DATE) is not NULL AND
		suspensao.ID_EVENTO_ATIVACAO is NULL AND
		CAST(suspensao.dtSuspensao AS DATE) < dtCicloInicio
	GROUP BY A.CODIGO_CONTRATO_AIR
),

soma_dias_suspensos_1 AS (
  SELECT
      uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
      SUM(suspensao_e_ativacao_dentro_do_ciclo.Dias) AS Dias_suspensao_e_ativacao_dentro_do_ciclo,
      SUM(comecou_suspenso_e_ativou_dentro_do_ciclo.Dias) AS Dias_comecou_suspenso_e_ativou_dentro_do_ciclo,
      SUM(suspenso_antes_do_ciclo_e_ativou_apos_ciclo.Dias) AS Dias_suspenso_antes_do_ciclo_e_ativou_apos_ciclo,
      SUM(suspenso_dentro_do_ciclo_e_ativou_apos_ciclo.Dias) AS Dias_suspenso_dentro_do_ciclo_e_ativou_apos_ciclo,
      SUM(suspenso_dentro_do_ciclo_e_ainda_esta_suspenso.Dias) AS Dias_suspenso_dentro_do_ciclo_e_ainda_esta_suspenso,
      SUM(suspenso_antes_do_ciclo_e_ainda_esta_suspenso.Dias) AS Dias_suspenso_antes_do_ciclo_e_ainda_esta_suspenso
  FROM uniao_basao_e_cancelados
  LEFT JOIN suspensao_e_ativacao_dentro_do_ciclo
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = suspensao_e_ativacao_dentro_do_ciclo.CODIGO_CONTRATO_AIR
  LEFT JOIN comecou_suspenso_e_ativou_dentro_do_ciclo
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = comecou_suspenso_e_ativou_dentro_do_ciclo.CODIGO_CONTRATO_AIR
  LEFT JOIN suspenso_antes_do_ciclo_e_ativou_apos_ciclo
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = suspenso_antes_do_ciclo_e_ativou_apos_ciclo.CODIGO_CONTRATO_AIR
  LEFT JOIN suspenso_dentro_do_ciclo_e_ativou_apos_ciclo
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = suspenso_dentro_do_ciclo_e_ativou_apos_ciclo.CODIGO_CONTRATO_AIR
  LEFT JOIN suspenso_dentro_do_ciclo_e_ainda_esta_suspenso
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = suspenso_dentro_do_ciclo_e_ainda_esta_suspenso.CODIGO_CONTRATO_AIR
  LEFT JOIN suspenso_antes_do_ciclo_e_ainda_esta_suspenso
      ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = suspenso_antes_do_ciclo_e_ainda_esta_suspenso.CODIGO_CONTRATO_AIR
  GROUP BY uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR
),

ajuste_dias_suspensos AS (
    SELECT CODIGO_CONTRATO_AIR,
        ((CASE WHEN Dias_suspensao_e_ativacao_dentro_do_ciclo IS NULL THEN 0 ELSE Dias_suspensao_e_ativacao_dentro_do_ciclo END) +
        (CASE WHEN Dias_comecou_suspenso_e_ativou_dentro_do_ciclo IS NULL THEN 0 ELSE Dias_comecou_suspenso_e_ativou_dentro_do_ciclo END) +
        (CASE WHEN Dias_suspenso_antes_do_ciclo_e_ativou_apos_ciclo IS NULL THEN 0 ELSE Dias_suspenso_antes_do_ciclo_e_ativou_apos_ciclo END) +
        (CASE WHEN Dias_suspenso_dentro_do_ciclo_e_ativou_apos_ciclo IS NULL THEN 0 ELSE Dias_suspenso_dentro_do_ciclo_e_ativou_apos_ciclo END) +
        (CASE WHEN Dias_suspenso_dentro_do_ciclo_e_ainda_esta_suspenso IS NULL THEN 0 ELSE Dias_suspenso_dentro_do_ciclo_e_ainda_esta_suspenso END) +
        (CASE WHEN Dias_suspenso_antes_do_ciclo_e_ainda_esta_suspenso IS NULL THEN 0 ELSE Dias_suspenso_antes_do_ciclo_e_ainda_esta_suspenso END)) AS dias_suspensos
    FROM soma_dias_suspensos_1
),

--------------------------------------
---- SERVIÇOS ADICIONAIS
--------------------------------------

servicos_adicionais_1 AS (
	SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    uniao_basao_e_cancelados.codigo_fatura_sydle,
	dim_faturas_itens.nome_item AS nome_servico_adicional,
	sum(dim_faturas_itens.valor_item) AS valor_servico_adicional
	FROM uniao_basao_e_cancelados
	LEFT JOIN {{ ref('dim_faturas_itens') }} as dim_faturas_itens
		ON uniao_basao_e_cancelados.codigo_fatura_sydle = dim_faturas_itens.codigo_fatura_sydle
	WHERE UPPER(dim_faturas_itens.nome_item) IN ('MUDANCA DE COMODO COM MATERIAL', 'LIGACOES EXCEDENTES')
    group by uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    uniao_basao_e_cancelados.codigo_fatura_sydle,
    dim_faturas_itens.nome_item
),

servicos_adicionais_rn AS (
	SELECT CODIGO_CONTRATO_AIR,
    codigo_fatura_sydle,
	nome_servico_adicional,
	valor_servico_adicional,
    ROW_NUMBER() OVER(PARTITION BY CODIGO_CONTRATO_AIR order by CODIGO_CONTRATO_AIR) as rn
	FROM servicos_adicionais_1
),

servicos_adicionais AS (
	SELECT CODIGO_CONTRATO_AIR,
    codigo_fatura_sydle,
	nome_servico_adicional,
	valor_servico_adicional,
    cast(case when nome_servico_adicional is not null then 1 else 0 end as int) AS servicos_adicionais
	FROM servicos_adicionais_rn
    where rn = 1
),

multa_rn AS (
	SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    uniao_basao_e_cancelados.codigo_fatura_sydle,
	 case
		when dim_faturas_itens.codigo_fatura_sydle is not null then 1
	 else 0 end AS multa,
	 CASE
		when UPPER(dim_faturas_itens.nome_item) like '%REPOS%' then 'Equipamento'
		when UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE' then 'Fidelidade'
		when UPPER(dim_faturas_itens.tipo_item)  = 'MULTA DE FIDELIDADE' and UPPER(dim_faturas_itens.nome_item) like '%REPOS%' then 'Equipamento e Fidelidade'
	 else NULL end AS tipo_multa,
     ROW_NUMBER() OVER(PARTITION BY uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
                                    uniao_basao_e_cancelados.codigo_fatura_sydle
                     order by uniao_basao_e_cancelados.codigo_fatura_sydle) as rn
	FROM uniao_basao_e_cancelados
	INNER JOIN {{ ref('dim_faturas_itens') }} as dim_faturas_itens
		ON uniao_basao_e_cancelados.codigo_fatura_sydle = dim_faturas_itens.codigo_fatura_sydle
	WHERE ((UPPER(dim_faturas_itens.nome_item) like '%REPOS%') OR (UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE'))
),

multa (
    SELECT CODIGO_CONTRATO_AIR,
    codigo_fatura_sydle,
    multa,
    tipo_multa
    FROM multa_rn
    WHERE rn = 1
),

--------------------------------------
---- PREENCHER DADOS DA VALIDAÇÃO DA APLICAÇÃO DE TI
--------------------------------------
validacao_aplicacao_distinct AS (
	SELECT distinct ID_FATURA_NUMERO,
	flValorCalculoFinal,
	stStatusValidacaoCiclo,
    ROW_NUMBER() OVER(PARTITION BY ID_FATURA_NUMERO order by ID_FATURA_NUMERO) as rn
	FROM {{ ref('vw_db_financeiro_tbl_aplicacao_status_validacao') }}
),

validacao_aplicacao AS (
	SELECT uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR,
    uniao_basao_e_cancelados.codigo_fatura_sydle,
	vw_db_financeiro_tbl_aplicacao_status_validacao.flValorCalculoFinal AS valor_calculo_final_API,
	vw_db_financeiro_tbl_aplicacao_status_validacao.stStatusValidacaoCiclo as validacao_API
	FROM uniao_basao_e_cancelados
	INNER JOIN validacao_aplicacao_distinct as vw_db_financeiro_tbl_aplicacao_status_validacao
		ON vw_db_financeiro_tbl_aplicacao_status_validacao.ID_FATURA_NUMERO = uniao_basao_e_cancelados.codigo_fatura_sydle
    where vw_db_financeiro_tbl_aplicacao_status_validacao.rn = 1
)

SELECT DISTINCT uniao_basao_e_cancelados.id_cliente_AIR
,uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR
,uniao_basao_e_cancelados.codigo_fatura_sydle
,uniao_basao_e_cancelados.emissao_fatura
,uniao_basao_e_cancelados.vencimento_fatura
,uniao_basao_e_cancelados.forma_pagamento_fatura
,uniao_basao_e_cancelados.data_pagamento_fatura
,uniao_basao_e_cancelados.mes_referencia_fatura
,uniao_basao_e_cancelados.mes_referencia_emissao
,uniao_basao_e_cancelados.dtCicloInicio
,uniao_basao_e_cancelados.dtCicloFim
,uniao_basao_e_cancelados.status_contrato
,uniao_basao_e_cancelados.nome_pacote
,uniao_basao_e_cancelados.codigo_pacote
,uniao_basao_e_cancelados.classificacao_fatura
,uniao_basao_e_cancelados.status_fatura
,uniao_basao_e_cancelados.data_ativacao_contrato
,uniao_basao_e_cancelados.data_cancelamento_contrato
,try_cast(uniao_basao_e_cancelados.aging_contrato as bigint) as aging_contrato
,uniao_basao_e_cancelados.cidade
,uniao_basao_e_cancelados.polo
,uniao_basao_e_cancelados.marca
,uniao_basao_e_cancelados.UF
,uniao_basao_e_cancelados.regiao
,uniao_basao_e_cancelados.regional
,uniao_basao_e_cancelados.sub_regional
,uniao_basao_e_cancelados.valor_base
,uniao_basao_e_cancelados.valor_final
,try_cast(uniao_basao_e_cancelados.valor_fatura as double) as valor_fatura
,uniao_basao_e_cancelados.segmento
,try_cast(uniao_basao_e_cancelados.ciclo as bigint) as ciclo
,try_cast(uniao_basao_e_cancelados.dia_vencimento as bigint) as dia_vencimento
,case when migracao.migracao_plano is null then 0 else migracao.migracao_plano end as migracao_plano
,migracao.data_migracao
,migracao.tipo_migracao
,migracao.natureza_migracao
,case when servicos_adicionais.servicos_adicionais is null then 0 else try_cast(servicos_adicionais.servicos_adicionais as bigint) end as servicos_adicionais
,servicos_adicionais.nome_servico_adicional
,try_cast(servicos_adicionais.valor_servico_adicional as string) as valor_servico_adicional
,case when multa.multa is null then 0 else try_cast(multa.multa as bigint) end as multa
,multa.tipo_multa
,try_cast(dias_base_calculo.dias_base_calculo as bigint) as dias_base_calculo
,try_cast(dias_base_calculo.valor_dia_base_calculo as string) as valor_dia_base_calculo
,try_cast(dados_desconto.porcentagem_desconto as string) as porcentagem_desconto
,try_cast(dados_desconto.quantidade_desconto as string) as quantidade_desconto
,try_cast(dados_desconto.valor_desconto as string) as valor_desconto
,try_cast(ajuste_dias_suspensos.dias_suspensos as bigint) as dias_suspensos
,(COALESCE(uniao_basao_e_cancelados.valor_final, uniao_basao_e_cancelados.valor_base) - (coalesce(valor_desconto, 0))) AS valor_liquido_contrato
,validacao_aplicacao.validacao_API
,try_cast(validacao_aplicacao.valor_calculo_final_API as string) as valor_calculo_final_API
,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM uniao_basao_e_cancelados
LEFT JOIN migracao ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = migracao.CODIGO_CONTRATO_AIR
LEFT JOIN dias_base_calculo ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = dias_base_calculo.CODIGO_CONTRATO_AIR
LEFT JOIN dados_desconto ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = dados_desconto.CODIGO_CONTRATO_AIR
LEFT JOIN ajuste_dias_suspensos ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = ajuste_dias_suspensos.CODIGO_CONTRATO_AIR
LEFT JOIN servicos_adicionais ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = servicos_adicionais.CODIGO_CONTRATO_AIR
    AND uniao_basao_e_cancelados.codigo_fatura_sydle = servicos_adicionais.codigo_fatura_sydle
LEFT JOIN multa ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = multa.CODIGO_CONTRATO_AIR
    AND uniao_basao_e_cancelados.codigo_fatura_sydle = multa.codigo_fatura_sydle
LEFT JOIN validacao_aplicacao ON uniao_basao_e_cancelados.CODIGO_CONTRATO_AIR = validacao_aplicacao.CODIGO_CONTRATO_AIR
    AND uniao_basao_e_cancelados.codigo_fatura_sydle = validacao_aplicacao.codigo_fatura_sydle

UNION ALL

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
,data_extracao
FROM {{ this }}
WHERE mes_referencia_emissao <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
