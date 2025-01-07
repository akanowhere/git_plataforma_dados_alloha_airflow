{% set catalog_schema_table=source("air_comercial", "tbl_perfilacao_retencao") %}
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
    id_contrato,
    primeiro_nivel,
    segundo_nivel,
    motivo,
    submotivo,
    observacao,
    servico,
    tipo_cliente,
    origem_contato

  FROM latest
)

SELECT *
FROM transformed
