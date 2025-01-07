{% set catalog_schema_table=source("sydle", "remessa") %}
{% set partition_column="id_remessa" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_remessa,
    FROM_JSON(
      parcelas,
      "ARRAY<STRUCT<
        numero: STRING,
        data_alteracao_status: STRING,
        valor: STRING,
        status: STRING
      >>"
    ) AS parcelas

  FROM latest
),

exploded_data AS (
  SELECT
    id_remessa,
    TRY_CAST(item.numero AS INT) AS parcela_numero,
    TRY_CAST(item.data_alteracao_status AS TIMESTAMP) AS parcela_data_alteracao_status,
    TRY_CAST(item.valor AS DOUBLE) AS parcela_valor,
    item.status AS parcela_status

  FROM parsed_data
    LATERAL VIEW EXPLODE(parcelas) AS item
)

SELECT *
FROM exploded_data
