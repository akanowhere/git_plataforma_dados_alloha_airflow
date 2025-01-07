{% set catalog_schema_table=source("air_comercial", "tbl_contrato_item") %}
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
    id_contrato_produto,
    valor_unitario,
    valor_base,
    quantidade,
    valor_final,
    item_codigo,
    item_nome,
    empresa_cnpj,
    empresa_nome,
    regra_desconto,
    valor_unitario_sap,
    indice_reajuste_sap

  FROM latest
)

SELECT *
FROM transformed
