SELECT 
    c1.id_contrato AS codigo_contrato_antigo,
    u.marca AS marca,
    c2.id_contrato AS codigo_contrato_novo,
    c1.id_cliente AS codigo_cliente,
    c1.data_cancelamento AS data_cancelamento_antigo,
    v.data_venda AS data_venda_novo,
    c2.data_primeira_ativacao AS data_ativacao_novo,
    DATEDIFF(day, c2.data_primeira_ativacao, c1.data_cancelamento) AS tempo_cancelamento_ativacao,
    e1.cep AS cep_antigo,
    e2.cep AS cep_novo,
    e1.numero AS numero_residencia_antigo,
    e2.numero AS numero_residencia_novo,
    v.canal_tratado AS canal,
    v.equipe,
    v.email_vendedor,
    e1.cidade
FROM 
    gold.base.dim_contrato c1
LEFT JOIN 
    gold.base.dim_contrato c2 
    ON c1.id_cliente = c2.id_cliente AND c2.id_contrato > c1.id_contrato
LEFT JOIN 
    gold.base.dim_contrato_entrega ce1 
    ON c1.id_contrato = ce1.id_contrato AND ce1.excluido = false
LEFT JOIN 
    gold.base.dim_endereco e1 
    ON ce1.id_endereco = e1.id_endereco AND e1.fonte = 'AIR'
LEFT JOIN 
    gold.base.dim_contrato_entrega ce2 
    ON c2.id_contrato = ce2.id_contrato AND ce2.excluido = false
LEFT JOIN 
    gold.base.dim_endereco e2 
    ON ce2.id_endereco = e2.id_endereco AND e2.fonte = 'AIR'
LEFT JOIN 
    gold.venda.fato_venda v 
    ON c2.id_contrato = v.id_contrato AND v.fonte = 'AIR'
LEFT JOIN 
    gold.base.dim_unidade u 
    ON e2.unidade = u.sigla AND u.excluido = false AND u.fonte = 'AIR'
WHERE 
    c1.data_primeira_ativacao IS NOT NULL 
    AND c2.data_primeira_ativacao >= '2024-01-01' 
    AND DATEDIFF(day, c2.data_primeira_ativacao, c1.data_cancelamento) BETWEEN 0 AND 40
    AND TRIM(REPLACE(REPLACE(e1.cep, '.', ''), '-', '')) = TRIM(REPLACE(REPLACE(e2.cep, '.', ''), '-', ''))
    AND TRIM(REPLACE(REPLACE(e1.numero, '.', ''), '-', '')) = TRIM(REPLACE(REPLACE(e2.numero, '.', ''), '-', ''))
