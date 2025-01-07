{% set catalog_schema_table=source('hub_sva_public', 'customers') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    contract AS id_contrato,
    document AS cpf_cnpj,
    lastname AS sobrenome,
    phone AS telefone,
    email,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    invoice_brand,
    brand_id,
    provider_id

  FROM latest
)

SELECT *
FROM
  transformed
