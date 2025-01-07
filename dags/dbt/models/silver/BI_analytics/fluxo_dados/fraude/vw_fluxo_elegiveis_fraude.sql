-- Acompanhamento de Churn e Possíveis Fraudes

-- 1) Contratos ativados/cancelados na regra de: 40d antes e 40d depois do cancelamento.
-- 2) apresentar esses contratos se, pelo menos um desses aparecer:
--   2a) mesmo cep e número no novo que o cancelado;
--   2b) mesmo cpf/cnpj do novo e do cancelado;
--   2c) mesmo nome da mãe do cliente;
--   2d) cliente atual ou cancelado tem parentesco pai/mãe, ou seja, se há relação de/para pai/mãe, ou filhos, entre os contratos cancelado e novo.
--   2e) Flag de mesmo telefones ou e-mails cadastrados para clientes diferentes

-- LAYOUT:
    -- CONTRATO NOVO	
    -- CONTRATO CANCELADO	
    -- DATA ATIVAÇÃO CONTRATO NOVO	
    -- DATA CANCELAMENTO CONTRATO CANCELADO	
    -- CEP IGUAL	
    -- NÚMERO IGUAL	
    -- NOME DA MÃE IGUAL	
    -- PARENTESCO
    -- VENDEDOR
    -- CANAL
    -- EQUIPE
    -- CIDADE
    -- REGIONAL
    -- FLAG CONTATO

-- Declarando inicio do Periodo

WITH temp_contratosElegiveis AS (
    SELECT  c1.id_contrato,
            c1.id_cliente,
            c1.data_primeira_ativacao,
            c1.data_cancelamento,
            try_cast(TRIM(REPLACE(REPLACE(e1.cep, '.', ''), '-', '')) AS BIGINT) AS cep,
            try_cast(REGEXP_REPLACE(e1.numero, '[^0-9]', '') AS BIGINT) AS numero,
            TRIM(UPPER(e1.cidade)) as cidade,
            e1.estado as estado,
            v.equipe,
            v.email_vendedor,
            v.nome_vendedor as vendedor,
            v.canal_tratado as canal,
            u.regional,
            TRIM(UPPER(cli.nome)) as nome,
            TRIM(UPPER(cli.nome_mae)) as nome_mae,
            COALESCE(cli.cpf,cli.cnpj) as cpf_cnpj,
            ct1.telefone_tratado AS telefone,
            cte1.contato AS email
    FROM {{ get_catalogo('gold') }}.base.dim_contrato c1
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente cli ON (c1.id_cliente = cli.id_cliente and cli.fonte = 'AIR')
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato_entrega ce1 ON (c1.id_contrato = ce1.id_contrato and ce1.excluido = FALSE)
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco e1 ON (ce1.id_endereco = e1.id_endereco and e1.fonte = 'AIR')
    LEFT JOIN {{ get_catalogo('gold') }}.contato.dim_contato_telefone ct1 ON (cli.id_cliente = ct1.id_cliente and ct1.excluido = '0' and ct1.telefone_tratado <> '99999999999')
    LEFT JOIN {{ get_catalogo('gold') }}.contato.dim_contato_email cte1 ON (cli.id_cliente = cte1.id_cliente and cte1.excluido = '0' and cte1.contato NOT LIKE 'atualize%')
    LEFT JOIN {{ get_catalogo('gold') }}.venda.fato_venda v ON (c1.id_contrato = v.id_contrato and v.fonte = 'AIR')
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u ON (e1.unidade = u.sigla and u.excluido= FALSE and u.fonte = 'AIR')
    WHERE c1.data_primeira_ativacao is not null and (c1.data_cancelamento >=  LAST_DAY(DATEADD(MONTH, -7, CURRENT_DATE())) OR c1.data_primeira_ativacao >= LAST_DAY(DATEADD(MONTH, -7, CURRENT_DATE())))
),
temp_contratosFraudes_cpfIgual AS (
    SELECT DISTINCT
            c1.id_contrato as CONTRATO_NOVO,
            c2.id_contrato as CONTRATO_CANCELADO,
            c1.data_primeira_ativacao as DATA_ATIVACAO_CONTRATO_NOVO,
            c2.data_cancelamento as DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            c1.vendedor as VENDEDOR,
            c1.canal as CANAL,
            c1.equipe as EQUIPE,
            c1.cidade as CIDADE,
            c1.regional as REGIONAL,
            c1.cep as CEP,
            c1.numero as NUMERO,
            c1.cpf_cnpj as CPF_CNPJ,
            c1.nome_mae as NOME_MAE,
            c1.nome as NOME_CLIENTE,
            c1.telefone as TELEFONE,
            c1.email as EMAIL,
            1 as FLG_CPF_CNPJ,
            CASE
                WHEN c1.numero= c2.numero and c1.cep = c2.cep THEN 1
                ELSE 0
            END FLG_ENDERECO,
            1 as FLG_PARENTE,
            CASE
                WHEN c1.telefone = c2.telefone THEN 1
                ELSE 0
            END FLG_TELEFONE,
            CASE
                WHEN c1.email = c2.email THEN 1
                ELSE 0
            END FLG_EMAIL
    FROM temp_contratosElegiveis c1
    INNER JOIN temp_contratosElegiveis c2 ON ( c1.id_contrato  > c2.id_contrato 
                                                AND c2.data_cancelamento IS NOT NULL 
                                                AND DATEDIFF(day, c2.data_cancelamento, c1.data_primeira_ativacao) BETWEEN -40 AND 40
                                                AND (c1.id_cliente = c2.id_cliente))
),
temp_contratosFraudes_enderecoIgual AS (
    SELECT DISTINCT
            c1.id_contrato as CONTRATO_NOVO,
            c2.id_contrato as CONTRATO_CANCELADO,
            c1.data_primeira_ativacao as DATA_ATIVACAO_CONTRATO_NOVO,
            c2.data_cancelamento as DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            c1.vendedor as VENDEDOR,
            c1.canal as CANAL,
            c1.equipe as EQUIPE,
            c1.cidade as CIDADE,
            c1.regional as REGIONAL,
            c1.cep as CEP,
            c1.numero as NUMERO,
            c1.cpf_cnpj as CPF_CNPJ,
            c1.nome_mae as NOME_MAE,
            c1.nome as NOME_CLIENTE,
            c1.telefone as TELEFONE,
            c1.email as EMAIL,
            CASE
                WHEN c1.id_cliente = c2.id_cliente THEN 1
                ELSE 0
            END FLG_CPF_CNPJ,
            1 FLG_ENDERECO,
            CASE
                WHEN c1.nome = c2.nome_mae THEN 1
                WHEN c2.nome = c1.nome_mae THEN 1
                WHEN c1.nome <> c1.nome and c1.nome_mae = c2.nome_mae THEN 1
                ELSE 0
            END FLG_PARENTE,
            CASE
                WHEN c1.telefone = c2.telefone THEN 1
                ELSE 0
            END FLG_TELEFONE,
            CASE
                WHEN c1.email = c2.email THEN 1
                ELSE 0
            END FLG_EMAIL
    FROM temp_contratosElegiveis c1
    INNER JOIN temp_contratosElegiveis c2 ON ( c1.id_contrato  > c2.id_contrato 
                                                AND c2.data_cancelamento IS NOT NULL 
                                                AND DATEDIFF(day, c2.data_cancelamento, c1.data_primeira_ativacao) BETWEEN -40 AND 40
                                                AND c1.cep = c2.cep 
                                                and c1.numero = c2.numero)
    WHERE c1.cep is not null and c2.numero is not null
),
temp_contratosFraudes_telefoneIgual AS (
    SELECT DISTINCT
            c1.id_contrato as CONTRATO_NOVO,
            c2.id_contrato as CONTRATO_CANCELADO,
            c1.data_primeira_ativacao as DATA_ATIVACAO_CONTRATO_NOVO,
            c2.data_cancelamento as DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            c1.vendedor as VENDEDOR,
            c1.canal as CANAL,
            c1.equipe as EQUIPE,
            c1.cidade as CIDADE,
            c1.regional as REGIONAL,
            c1.cep as CEP,
            c1.numero as NUMERO,
            c1.cpf_cnpj as CPF_CNPJ,
            c1.nome_mae as NOME_MAE,
            c1.nome as NOME_CLIENTE,
            c1.telefone as TELEFONE,
            c1.email as EMAIL,
            CASE
                WHEN c1.id_cliente = c2.id_cliente THEN 1
                ELSE 0
            END FLG_CPF_CNPJ,
            CASE
                WHEN c1.numero= c2.numero and c1.cep = c2.cep THEN 1
                ELSE 0
            END FLG_ENDERECO,
            CASE
                WHEN c1.nome = c2.nome_mae THEN 1
                WHEN c2.nome = c1.nome_mae THEN 1
                WHEN c1.nome <> c1.nome and c1.nome_mae = c2.nome_mae THEN 1
                ELSE 0
            END FLG_PARENTE,
            1 FLG_TELEFONE,
            CASE
                WHEN c1.email = c2.email THEN 1
                ELSE 0
            END FLG_EMAIL
    FROM temp_contratosElegiveis c1
    INNER JOIN temp_contratosElegiveis c2 ON ( c1.id_contrato  > c2.id_contrato 
                                                AND c2.data_cancelamento IS NOT NULL 
                                                AND DATEDIFF(day, c2.data_cancelamento, c1.data_primeira_ativacao) BETWEEN -40 AND 40
                                                AND c1.telefone = c2.telefone)
    WHERE c1.telefone is not null and c2.telefone is not null
),
temp_contratosFraudes_emailIgual AS (
    SELECT DISTINCT
            c1.id_contrato as CONTRATO_NOVO,
            c2.id_contrato as CONTRATO_CANCELADO,
            c1.data_primeira_ativacao as DATA_ATIVACAO_CONTRATO_NOVO,
            c2.data_cancelamento as DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            c1.vendedor as VENDEDOR,
            c1.canal as CANAL,
            c1.equipe as EQUIPE,
            c1.cidade as CIDADE,
            c1.regional as REGIONAL,
            c1.cep as CEP,
            c1.numero as NUMERO,
            c1.cpf_cnpj as CPF_CNPJ,
            c1.nome_mae as NOME_MAE,
            c1.nome as NOME_CLIENTE,
            c1.telefone as TELEFONE,
            c1.email AS EMAIL,
            CASE
                WHEN c1.id_cliente = c2.id_cliente THEN 1
                ELSE 0
            END FLG_CPF_CNPJ,
            CASE
                WHEN c1.numero= c2.numero and c1.cep = c2.cep THEN 1
                ELSE 0
            END FLG_ENDERECO,
            CASE
                WHEN c1.nome = c2.nome_mae THEN 1
                WHEN c2.nome = c1.nome_mae THEN 1
                WHEN c1.nome <> c1.nome and c1.nome_mae = c2.nome_mae THEN 1
                ELSE 0
            END FLG_PARENTE,
            CASE
                WHEN c1.telefone = c2.telefone THEN 1
                ELSE 0
            END FLG_TELEFONE,
            1 FLG_EMAIL
    FROM temp_contratosElegiveis c1
    INNER JOIN temp_contratosElegiveis c2 ON ( c1.id_contrato  > c2.id_contrato 
                                                AND c2.data_cancelamento IS NOT NULL 
                                                AND DATEDIFF(day, c2.data_cancelamento, c1.data_primeira_ativacao) BETWEEN -40 AND 40
                                                AND c1.email = c2.email)
    WHERE c1.email is not null and c2.email is not null
)
SELECT  A.CONTRATO_NOVO,
        A.CONTRATO_CANCELADO,
        A.DATA_ATIVACAO_CONTRATO_NOVO,
        A.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
        A.VENDEDOR,
        A.CANAL,
        A.EQUIPE,
        A.CIDADE,
        A.REGIONAL,
        MAX(FLG_CPF_CNPJ) as FLG_CPF_CNPJ,
        MAX(FLG_ENDERECO) as FLG_ENDERECO,
        MAX(FLG_PARENTE) as FLG_PARENTE,
        MAX(FLG_TELEFONE) as FLG_TELEFONE,
        MAX(FLG_EMAIL) as FLG_EMAIL
FROM (
    SELECT  t1.CONTRATO_NOVO,
            t1.CONTRATO_CANCELADO,
            t1.DATA_ATIVACAO_CONTRATO_NOVO,
            t1.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            t1.VENDEDOR,
            t1.CANAL,
            t1.EQUIPE,
            t1.CIDADE,
            t1.REGIONAL,
            t1.CEP,
            t1.NUMERO,
            t1.CPF_CNPJ,
            t1.NOME_MAE,
            t1.NOME_CLIENTE,
            t1.TELEFONE,
            t1.EMAIL,
            t1.FLG_CPF_CNPJ,
            t1.FLG_ENDERECO,
            t1.FLG_PARENTE,
            t1.FLG_TELEFONE,
            t1.FLG_EMAIL
    FROM temp_contratosFraudes_cpfIgual t1
    UNION ALL
    SELECT  t1.CONTRATO_NOVO,
            t1.CONTRATO_CANCELADO,
            t1.DATA_ATIVACAO_CONTRATO_NOVO,
            t1.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            t1.VENDEDOR,
            t1.CANAL,
            t1.EQUIPE,
            t1.CIDADE,
            t1.REGIONAL,
            t1.CEP,
            t1.NUMERO,
            t1.CPF_CNPJ,
            t1.NOME_MAE,
            t1.NOME_CLIENTE,
            t1.TELEFONE,
            t1.EMAIL,
            t1.FLG_CPF_CNPJ,
            t1.FLG_ENDERECO,
            t1.FLG_PARENTE,
            t1.FLG_TELEFONE,
            t1.FLG_EMAIL
    FROM temp_contratosFraudes_enderecoIgual t1
    UNION ALL
    SELECT  t1.CONTRATO_NOVO,
            t1.CONTRATO_CANCELADO,
            t1.DATA_ATIVACAO_CONTRATO_NOVO,
            t1.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            t1.VENDEDOR,
            t1.CANAL,
            t1.EQUIPE,
            t1.CIDADE,
            t1.REGIONAL,
            t1.CEP,
            t1.NUMERO,
            t1.CPF_CNPJ,
            t1.NOME_MAE,
            t1.NOME_CLIENTE,
            t1.TELEFONE,
            t1.EMAIL,
            t1.FLG_CPF_CNPJ,
            t1.FLG_ENDERECO,
            t1.FLG_PARENTE,
            t1.FLG_TELEFONE,
            t1.FLG_EMAIL
    FROM temp_contratosFraudes_telefoneIgual t1
    UNION ALL
    SELECT  t1.CONTRATO_NOVO,
            t1.CONTRATO_CANCELADO,
            t1.DATA_ATIVACAO_CONTRATO_NOVO,
            t1.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
            t1.VENDEDOR,
            t1.CANAL,
            t1.EQUIPE,
            t1.CIDADE,
            t1.REGIONAL,
            t1.CEP,
            t1.NUMERO,
            t1.CPF_CNPJ,
            t1.NOME_MAE,
            t1.NOME_CLIENTE,
            t1.TELEFONE,
            t1.EMAIL,
            t1.FLG_CPF_CNPJ,
            t1.FLG_ENDERECO,
            t1.FLG_PARENTE,
            t1.FLG_TELEFONE,
            t1.FLG_EMAIL
    FROM temp_contratosFraudes_emailIgual t1
) A
GROUP BY A.CONTRATO_NOVO,
        A.CONTRATO_CANCELADO,
        A.DATA_ATIVACAO_CONTRATO_NOVO,
        A.DATA_CANCELAMENTO_CONTRATO_CANCELADO,
        A.VENDEDOR,
        A.CANAL,
        A.EQUIPE,
        A.CIDADE,
        A.REGIONAL;