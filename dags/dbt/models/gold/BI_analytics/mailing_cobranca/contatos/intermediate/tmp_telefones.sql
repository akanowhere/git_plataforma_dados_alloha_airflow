{{ 
    config(
        materialized='ephemeral'
    ) 
}}

WITH cte_telefones AS (
    SELECT
        id_cliente,
        CONCAT('+55', telefone_tratado) AS telefone,
        ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS prioridade
    FROM  {{ ref('dim_contato_telefone') }}
    WHERE 
        excluido = 0
        AND classificacao_contato IN ('MOVEL', 'FIXO')
        AND existe_air = 'S'
        AND NOT fonte_contato LIKE 'sumicity%'
)

SELECT 
    id_cliente,
    telefone,
    CASE WHEN prioridade = 1 THEN telefone ELSE NULL END AS tel_01,
    CASE WHEN prioridade = 2 THEN telefone ELSE NULL END AS tel_02,
    CASE WHEN prioridade = 3 THEN telefone ELSE NULL END AS tel_03,
    CASE WHEN prioridade = 4 THEN telefone ELSE NULL END AS tel_04,
    CASE WHEN prioridade = 5 THEN telefone ELSE NULL END AS tel_05,
    CASE WHEN prioridade = 6 THEN telefone ELSE NULL END AS tel_06
FROM cte_telefones