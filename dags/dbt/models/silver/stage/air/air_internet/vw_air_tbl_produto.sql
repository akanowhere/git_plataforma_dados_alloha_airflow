{% set catalog_schema_table=source("air_internet", "tbl_produto") %}
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
    sap_produto_codigo,
    sap_produto_nome,
    id_banda,
    login_limite

  FROM latest
)

SELECT *
FROM transformed
