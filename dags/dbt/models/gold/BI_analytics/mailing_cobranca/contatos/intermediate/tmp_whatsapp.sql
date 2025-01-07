{{ 
    config(
        materialized='ephemeral'
    ) 
}}

SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS whatsapp
FROM  {{ ref('dim_contato_telefone') }}
WHERE fonte_contato LIKE 'sumicity%'