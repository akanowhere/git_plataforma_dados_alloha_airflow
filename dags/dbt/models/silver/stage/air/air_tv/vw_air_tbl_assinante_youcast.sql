{% set catalog_schema_table=source("air_tv", "tbl_assinante_youcast") %}
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
    cliente_id,
    contrato_id,
    cpf_cnpj,
    produto_id,
    ativo,
    email,
    telefone,
    integracao_hub_status,
    integracao_transacao,
    integracao_status,
    integracao_mensagem

  FROM latest
)

SELECT *
FROM transformed
