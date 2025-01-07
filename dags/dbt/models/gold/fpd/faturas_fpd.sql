WITH tb_endereco_contrato AS (
SELECT E.estado ,E.cidade ,E.unidade, A.id_contrato_air
from {{ get_catalogo('gold') }}.base.dim_contrato A 
INNER JOIN (
    SELECT min(id_endereco) as endereco, id_contrato
    FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega
    group by id_contrato
) as tb_temp ON tb_temp.id_contrato = A.id_contrato_air
INNER JOIN {{ get_catalogo('gold') }}.base.dim_endereco E 
on E.id_endereco = tb_temp.endereco
),

atualizacao_inadim as (
SELECT
codigo_fatura_sydle,
MIN(data_atualizacao) AS data_atualizacao_inadim
FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_all_versions
WHERE UPPER(status_fatura) <> 'EMITIDA'
GROUP BY codigo_fatura_sydle
),

atualizacao_fb as (
SELECT
codigo_fatura_sydle,
MIN(data_atualizacao) AS data_atualizacao_fb
FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_all_versions
WHERE UPPER(status_fatura) NOT IN ('PAGA', 'NEGOCIADA COM O CLIENTE', 'EMITIDA', 'EM EMISSÃO', 'BAIXA OPERACIONAL', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO', 'AGUARDANDO BAIXA EM SISTEMA EXTERNO')
GROUP BY codigo_fatura_sydle
),

final AS (
    SELECT DISTINCT
    A.contrato_air
    ,B.id_contrato_air
    ,'AIR' AS fonte
    ,A.codigo_fatura_sydle
    ,A.data_criacao
    ,A.data_vencimento
    ,A.data_pagamento
    ,A.valor_sem_multa_juros
    ,A.valor_pago
    ,COALESCE(A.marca, G.marca) as marca
    ,A.classificacao
    ,A.status_fatura
    ,A.data_atualizacao
    ,COALESCE(atualizacao_inadim.data_atualizacao_inadim, current_date()) as dt_atualizacao_inadim
    ,COALESCE(atualizacao_fb.data_atualizacao_fb, current_date()) as dt_atualizacao_fb
    ,DATEDIFF(day, A.data_vencimento , (COALESCE (A.data_pagamento, atualizacao_inadim.data_atualizacao_inadim, current_date()))) AS aging_inadim
    ,DATEDIFF(day, A.data_vencimento , (COALESCE (atualizacao_fb.data_atualizacao_fb, current_date()))) AS aging_fb
    ,C.canal_tratado as canal_venda
    ,C.equipe as equipe_venda
    ,C.id_vendedor
    ,D.nome as vendedor_nome
    ,G.regional
    ,G.cluster
    ,tb_endereco_contrato.estado
    ,tb_endereco_contrato.cidade
    ,tb_endereco_contrato.unidade
    ,CASE
    WHEN B.b2b = TRUE THEN 'B2B'
    WHEN B.pme = TRUE THEN 'PME'
    ELSE 'B2C' end AS segmento
    ,F.tipo as tipo_cliente
    ,B.data_primeira_ativacao AS data_ativacao_contrato
    ,COALESCE(F.cpf, F.cnpj) as cpf_cnpj

    FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing A

    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato B ON B.id_contrato_air= A.contrato_air

    LEFT JOIN {{ get_catalogo('gold') }}.venda.fato_venda C
    ON CASE
    WHEN B.id_contrato_legado is not null THEN B.id_contrato_legado
    ELSE B.id_contrato_air end 
    =
    CASE
    WHEN C.id_contrato_ixc is not null THEN C.id_contrato_ixc
    ELSE C.id_contrato_air end 
        AND C.marca = A.marca

    LEFT JOIN {{ get_catalogo('gold') }}.venda.dim_vendedor D ON C.id_vendedor = D.id_vendedor AND C.fonte = D.fonte

    LEFT JOIN tb_endereco_contrato ON tb_endereco_contrato.id_contrato_air = A.contrato_air

    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente F ON F.id_cliente = COALESCE(A.id_cliente_air, B.id_cliente) AND F.fonte = 'AIR'

    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade G ON G.sigla = tb_endereco_contrato.unidade AND G.fonte = 'AIR'

    LEFT JOIN atualizacao_inadim ON atualizacao_inadim.codigo_fatura_sydle = A.codigo_fatura_sydle

    LEFT JOIN atualizacao_fb ON atualizacao_fb.codigo_fatura_sydle = A.codigo_fatura_sydle

    WHERE ((UPPER(A.status_fatura) <> 'CANCELADA') OR (UPPER(A.status_fatura) = 'CANCELADA' AND date(atualizacao_fb.data_atualizacao_fb) >= date(A.data_vencimento))) AND 
    UPPER(A.classificacao) = 'INICIAL' AND date(A.data_vencimento) >= date_add(current_date(), -100) and DATEDIFF(day, date(A.data_vencimento) , (COALESCE (date(atualizacao_fb.data_atualizacao_fb), date(current_date())))) >= 0
)

SELECT contrato_air
,id_contrato_air
,fonte
,codigo_fatura_sydle
,data_criacao
,data_vencimento
,data_pagamento
,valor_sem_multa_juros
,valor_pago
,marca
,classificacao
,status_fatura
,data_atualizacao
,dt_atualizacao_inadim
,dt_atualizacao_fb
,aging_inadim
,aging_fb
,canal_venda
,equipe_venda
,id_vendedor
,vendedor_nome
,regional
,cluster
,estado
,cidade
,unidade
,segmento
,tipo_cliente
,data_ativacao_contrato
,cpf_cnpj
,CASE
  WHEN aging_fb >= 0 THEN 1
  ELSE 0
END AS FLG_FB
,CASE
  WHEN aging_inadim >= 1 THEN 1
  ELSE 0
END AS FLG_FPD1D
,CASE
  WHEN aging_inadim >= 5 THEN 1
  ELSE 0
END AS FLG_FPD5D
,CASE
  WHEN aging_inadim >= 10 THEN 1
  ELSE 0
END AS FLG_FPD10D
,CASE
  WHEN aging_inadim >= 15 THEN 1
  ELSE 0
END AS FLG_FPD15D
,CASE
  WHEN aging_inadim >= 20 THEN 1
  ELSE 0
END AS FLG_FPD20D
,CASE
  WHEN aging_inadim >= 30 THEN 1
  ELSE 0
END AS FLG_FPD30D
,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM final

UNION

SELECT contrato_air
,id_contrato_air
,fonte
,codigo_fatura_sydle
,data_criacao
,data_vencimento
,data_pagamento
,valor_sem_multa_juros
,valor_pago
,marca
,classificacao
,status_fatura
,data_atualizacao
,dt_atualizacao_inadim
,dt_atualizacao_fb
,aging_inadim
,aging_fb
,canal_venda
,equipe_venda
,id_vendedor
,vendedor_nome
,regional
,cluster
,estado
,cidade
,unidade
,segmento
,tipo_cliente
,data_ativacao_contrato
,cpf_cnpj
,FLG_FB
,FLG_FPD1D
,FLG_FPD5D
,FLG_FPD10D
,FLG_FPD15D
,FLG_FPD20D
,FLG_FPD30D
,data_extracao
FROM {{ get_catalogo('gold') }}.fpd.faturas_fpd
WHERE date(data_vencimento) < date_add(current_date(), -100)
