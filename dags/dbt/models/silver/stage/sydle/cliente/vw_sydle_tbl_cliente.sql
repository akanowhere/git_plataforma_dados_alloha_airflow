{% set catalog_schema_table=source("sydle", "cliente") %}
{% set partition_column="id_cliente" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_cliente,
    nome,
    razao_social,
    documentos,
    emails,
    email_principal,
    telefones,
    enderecos,
    TRY_CAST(data_nascimento AS TIMESTAMP) AS data_nascimento,
    estado_civil,
    nacionalidade,
    naturalidade,
    filiacao,
    redes_sociais,
    TRY_CAST(falecido AS BOOLEAN) AS falecido,
    sigla,
    tags,
    codigo_externo,
    TRY_CAST(data_integracao AS TIMESTAMP) AS data_integracao,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

  FROM latest
)

SELECT *
FROM transformed
