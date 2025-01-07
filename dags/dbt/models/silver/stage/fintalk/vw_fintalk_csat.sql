WITH source AS (
  SELECT DISTINCT user_data2 FROM {{ source('fintalk_alloha', 'customers') }}
  WHERE user_data2 <> 'None'
),

parsed_user_data2 AS (
  SELECT
    from_json(
      user_data2,
      'STRUCT<
        `Data / Hora`: STRING,
        userId: STRING,
        `CPF / CNPJ`: STRING,
        Nome: STRING,
        Resolvido: STRING,
        Facilidade: STRING,
        Nota: STRING,
        Origem: STRING
      >'
    ) AS csat_json

  FROM source
),

transformed AS (
  SELECT
    csat_json["userId"] AS user_id,
    TO_TIMESTAMP(csat_json["Data / Hora"], 'dd/MM/yyyy HH:mm:ss') AS data_hora,
    NULLIF(REGEXP_REPLACE(csat_json["CPF / CNPJ"], '[^0-9]', ''), '') AS cpf_cnpj,
    TRIM(csat_json["Nome"]) AS nome,
    NULLIF(csat_json["Resolvido"], '') AS resolvido,
    NULLIF(csat_json["Facilidade"], '') AS facilidade,
    TRY_CAST(csat_json["Nota"] AS INT) AS nota,
    NULLIF(csat_json["Origem"], '') AS origem

  FROM parsed_user_data2
)

SELECT *
FROM
  transformed
