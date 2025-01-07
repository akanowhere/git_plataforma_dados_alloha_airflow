WITH source AS (
  SELECT DISTINCT user_data4 FROM {{ source('fintalk_alloha', 'customers') }}
  WHERE user_data4 <> 'None'
),

parsed_user_data4 AS (
  SELECT
    get_json_object(user_data4, '$.cpfCnpj') AS cpf_cnpj,
    get_json_object(user_data4, '$.Contrato') AS contrato,
    get_json_object(user_data4, '$.novo-plano') AS novo_plano,
    get_json_object(user_data4, '$.preco') AS preco,
    get_json_object(user_data4, '$.codigo') AS codigo,
    get_json_object(user_data4, '$.campanha') AS campanha,
    COALESCE(
      get_json_object(user_data4, '$.Data/Hora'),
      get_json_object(user_data4, '$.Data / Hora')
    ) AS data_hora,
    get_json_object(user_data4, '$.Vencimento') AS vencimento,
    get_json_object(user_data4, '$.Forma-de-pagamento') AS forma_pagamento,
    get_json_object(user_data4, '$.plano') AS plano,
    get_json_object(user_data4, '$.recusa') AS recusa

  FROM source
),

transformed AS (
  SELECT
    NULLIF(REGEXP_REPLACE(cpf_cnpj, '[^0-9]', ''), '') AS cpf_cnpj,
    TRY_CAST(contrato AS INT) AS contrato,
    COALESCE(
        novo_plano,
        CASE WHEN TRY_CAST(plano AS INT) IS NULL THEN plano END
    ) AS novo_plano,
    preco,
    codigo,
    campanha,
    TO_TIMESTAMP(data_hora, 'dd/MM/yyyy HH:mm:ss') AS data_hora,
    TRY_CAST(vencimento AS INT) AS vencimento,
    forma_pagamento,
    TRY_CAST(plano AS INT) AS plano,
    TRY_CAST(COALESCE(recusa, false) AS BOOLEAN) AS recusa

  FROM parsed_user_data4
)

SELECT *
FROM
  transformed
