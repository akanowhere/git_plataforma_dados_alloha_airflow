{{ 
    config(
        materialized='ephemeral'
    ) 
}}

--### Separar dados de unidade / cidade / regional ###--
WITH tmp_unidade AS (
    SELECT 
        id_unidade,
        sigla,
        UPPER(marca) marca,
        UPPER(nome) cidade,
        REPLACE(REPLACE(regional,'EGIAO_0',''),'EGIAO-0','') regional,
        sigla as unidade,
        'BR' localidade
    FROM {{ ref('dim_unidade') }}
    WHERE sigla IS NOT NULL
    GROUP BY id_unidade, sigla, marca, nome, regional
    HAVING id_unidade IS NOT NULL
)

--### Separar clientes que possuem algum contrato B2B ou contratos próprios para expurgo  ou Permuta/Cortesia ###--

-- PERMUTAS/CORTESIAS
, tmp_permuta_cortesia AS (
    SELECT
        DISTINCT
        co.id_cliente,
        co.id_contrato
    FROM {{ ref('dim_contrato') }} co
    LEFT JOIN {{ ref('dim_contrato_campanha') }} cca
    ON co.id_contrato = cca.id_contrato
    LEFT JOIN {{ ref('dim_campanha') }} ca
        ON cca.id_campanha = ca.id_campanha 
    WHERE 
        ca.nome LIKE UPPER('%colaborad%')
        OR  ca.nome LIKE UPPER('%permut%') 
        OR ca.nome LIKE UPPER('%cortesia%')
)

-- B2B
, tmp_clientes_b2b AS (
    SELECT 
        id_cliente,
        id_contrato
    FROM {{ ref('dim_contrato') }}
    WHERE b2b = true
)
--  PRÓPRIOS
, tmp_clientes_proprios AS(
    SELECT 
        co.id_cliente,
        co.id_contrato
    FROM {{ ref('dim_contrato') }} co
    LEFT JOIN {{ ref('dim_cliente') }} cli
        ON co.id_cliente = cli.id_cliente
    WHERE 
        cli.nome LIKE '%SUMICITY%'
        OR cli.nome LIKE '%VELOMAX%'
        OR cli.nome LIKE '%OPENLINK%'
)

-- Legado Mob não cobrável
, tmp_clientes_mob_nao_cobraveis AS (
	SELECT 
        contrato_air as id_contrato,
        id_cliente
    FROM {{ get_catalogo('silver') }}.stage_legado.vw_mailing_base_legado_nao_cobravel_mob m
    LEFT JOIN gold_dev.base.dim_contrato c
        ON m.contrato_air = c.id_contrato_air
)

, tmp_clientes_expurgo AS (
    SELECT DISTINCT *
    FROM (
        SELECT * FROM tmp_permuta_cortesia
        UNION ALL
        SELECT * FROM tmp_clientes_b2b
        UNION ALL
        SELECT * FROM tmp_clientes_proprios
        UNION ALL
        SELECT * FROM tmp_clientes_mob_nao_cobraveis
    ) AS combined_results
)

--### Separar contratos elegiveis ao mailing ###--
, all_contratos AS (
    SELECT
        c.id_contrato, 
        c.id_cliente,
        c.status status_contrato,
        CASE 
             WHEN status = 'ST_CONT_CANCELADO' 
                THEN 'INATIVO'
            ELSE 'ATIVO'
        END status_mailing,
        c.unidade_atendimento,
        c.b2b,
        c.VERSAO_CONTRATO_AIR  AS versao_contrato
    FROM {{ ref('dim_contrato') }} c
    WHERE c.data_primeira_ativacao IS NOT NULL
)
SELECT 
    c.id_contrato,
    c.status_contrato,
    c.id_cliente,
    c.status_mailing,
    c.versao_contrato,
    u.marca,
    u.cidade,
    u.regional,
    u.localidade
FROM all_contratos c
ANTI JOIN tmp_clientes_expurgo ex
    ON c.id_cliente = ex.id_cliente AND c.id_contrato = ex.id_contrato
LEFT JOIN tmp_unidade u
    ON c.unidade_atendimento = u.sigla
