{{ 
    config(
        materialized='ephemeral'
    ) 
}}

 SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS telefone,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS prioridade
FROM  {{ ref('dim_contato_telefone') }}
WHERE 
    excluido = 0
    AND classificacao_contato IN ('MOVEL')
    AND existe_air = 'S'
    AND NOT fonte_contato LIKE 'sumicity%'