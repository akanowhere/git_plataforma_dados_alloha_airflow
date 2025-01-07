{% set catalog_schema_table=source("air_chamado", "tbl_os_tecnico") %}
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
    codigo_usuario,
    nome,
    codigo_deposito,
    posicao_estoque,
    ativo,
    terceirizado,
    id_terceirizada,
    telefone,
    bloqueado_provisionamento_app,
    id_ofs,
    email

  FROM latest
)

SELECT *
FROM transformed
