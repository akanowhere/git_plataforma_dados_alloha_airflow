SELECT *
FROM {{ source('loja', 'loja_stats') }}
