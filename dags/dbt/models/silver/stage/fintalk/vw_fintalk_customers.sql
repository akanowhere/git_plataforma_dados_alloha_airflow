WITH source AS (
  SELECT * FROM {{ source('fintalk_alloha', 'customers') }}
),

transformed AS (
  SELECT DISTINCT
    id,
    session_id,
    NULLIF(name, 'None') AS name,
    NULLIF(nickname, 'None') AS nickname,
    NULLIF(email, 'None') AS email,
    NULLIF(cpf, 'None') AS cpf,
    phone_number,
    NULLIF(birthdate, 'None') AS birthdate,
    whatsapp_id,
    app_client,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    stage,
    bot

  FROM source
)

SELECT *
FROM transformed
