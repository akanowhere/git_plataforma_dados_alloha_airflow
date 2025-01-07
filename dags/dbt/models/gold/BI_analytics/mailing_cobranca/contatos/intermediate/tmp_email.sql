{{ 
    config(
        materialized='ephemeral'
    ) 
}}


SELECT
    id_cliente,    
    contato AS email,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS prioridade
FROM  {{ ref('dim_contato_email') }}
WHERE 
tipo = 'EMAIL' 
AND excluido = 0 
AND NOT (
    contato NOT LIKE '%@%' 
    OR contato LIKE '%atualiz%' 
    OR contato LIKE '%sac@sumicity%' 
    OR contato LIKE '%autalize%' 
    OR contato LIKE '%teste%' 
    OR contato LIKE '%naotem%' 
    OR contato LIKE '%n_oconsta%' 
    OR contato LIKE '%rca@%' 
    OR contato LIKE '%sumi%' 
    OR contato LIKE '%@tem%'
)