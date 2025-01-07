WITH temp_contrato_sydle AS (
    SELECT
        t1.codigo_externo AS contrato_air,
        t1.status_status_servico,
        t1.tag AS tipo_contrato,
        t1.data_ativacao AS data_ativacao_contrato,
        t1.plano_identificador AS codigo_plano,
        t1.plano_nome AS nome_plano,
        t2.data_de_ativacao AS data_ativacao_componente,
        t2.tarifador,
        t2.produto_nome,
        t2.produto_adicional,
        t2.precificacao_valor,
        t2.precificacao_valor - (t2.precificacao_valor * t3.desconto_precificacao_valor) / 100 AS precificacao_valor_com_desconto,
        t3.desconto_nome,
        t3.desconto_precificacao_valor,
        t3.data_inicio,
        t3.data_termino,
        t1.endereco_entrega_cidade_nome AS cidade,
        t1.endereco_entrega_estado_sigla AS uf,
        t1.data_extracao,
        t1.unidade_atendimento_marca_associada AS marca
    FROM {{ get_catalogo('gold') }}.sydle.dim_contrato_sydle t1 
    LEFT JOIN {{ get_catalogo('gold') }}.sydle.dim_contrato_componentes t2 
        ON (t1.id_contrato = t2.id_contrato AND (t2.tarifador <> 'Valor por parcela' OR t2.tarifador IS NULL))
    LEFT JOIN {{ get_catalogo('gold') }}.sydle.dim_contrato_descontos t3 
        ON (t2.id_contrato = t3.id_contrato AND t2.produto_identificador_sydle = t3.produto_identificador_sydle)
    WHERE t1.status_status_servico <> 'Cancelado'
)

, temp_contrato_sydle_valor AS (
    SELECT
        contrato_air,
        SUM(precificacao_valor) AS VALOR_PLANO,
        SUM(precificacao_valor_com_desconto) AS VALOR_PLANO_DESCONTO,
        CAST(MAX(data_extracao) AS DATE) AS DATA_ATUALIZACAO
    FROM temp_contrato_sydle
    GROUP BY contrato_air
)

SELECT
    t1.contrato_air AS codigo_contrato_air,
    t1.codigo_plano,
    t1.nome_plano,
    ROUND(t2.VALOR_PLANO,2) AS valor_plano,
    ROUND(COALESCE(t2.VALOR_PLANO_DESCONTO, t2.VALOR_PLANO),2) AS valor_plano_desconto,
    t1.produto_nome AS nome_produto,
    t1.produto_adicional AS flag_produto_adicional,
    ROUND(t1.precificacao_valor,2) AS valor_produto,
    ROUND(COALESCE(t1.precificacao_valor_com_desconto, t1.precificacao_valor),2) AS valor_produto_desconto,
    ROUND(COALESCE(t1.desconto_precificacao_valor, 0),2) AS desconto_produto,
    t1.data_ativacao_contrato AS data_ativacao_contrato,
    t1.data_ativacao_componente AS data_ativacao_componente,
    t1.cidade AS cidade,
    t1.uf AS uf,
    t1.marca AS marca,
    t1.status_status_servico AS status_servico_sydle,
    t1.tipo_contrato AS tipo_contrato,
    date_sub(current_date(), 1) AS data_referencia
FROM temp_contrato_sydle t1
LEFT JOIN temp_contrato_sydle_valor t2 
    ON (t1.contrato_air = t2.contrato_air)
