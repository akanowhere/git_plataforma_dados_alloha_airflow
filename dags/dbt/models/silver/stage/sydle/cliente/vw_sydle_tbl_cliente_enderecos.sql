{% set catalog_schema_table=source("sydle", "cliente") %}
{% set partition_column="id_cliente" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_cliente,
    FROM_JSON(
      enderecos,
      "ARRAY<STRUCT<
        cidade_nome: STRING,
        tipo: STRING,
        complemento: STRING,
        numero: STRING,
        logradouro: STRING,
        bairro: STRING,
        estado_nome: STRING,
        referencia: STRING,
        cidade_codigo_ibge: STRING,
        estado_sigla: STRING,
        cep: STRING,
        pais: STRING
      >>"
    ) AS enderecos

  FROM latest
),

exploded_data AS (
  SELECT
    id_cliente,
    item.cidade_nome,
    item.tipo,
    item.complemento,
    item.numero,
    item.logradouro,
    item.bairro,
    item.estado_nome,
    item.referencia,
    item.cidade_codigo_ibge,
    item.estado_sigla,
    item.cep,
    item.pais

  FROM parsed_data
    LATERAL VIEW EXPLODE(enderecos) AS item
)

SELECT *
FROM exploded_data
