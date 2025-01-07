{% set catalog_schema_table=source("air_comercial", "tbl_cliente_contato") %}
{% set partition_column="id" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    usuario_criacao,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    usuario_alteracao,
    excluido,
    id_cliente,
    tipo,
    contato,
    confirmado,
    status,
    codigo_confirmacao,
    favorito,
    canal_confirmacao,
    TRY_CAST(data_alter_contato AS TIMESTAMP) AS data_alter_contato,
    TRY_CAST(data_confirmacao AS TIMESTAMP) AS data_confirmacao,
    origem

  FROM latest
)

SELECT *
FROM transformed
