{% set catalog_schema_table=source("air_comercial", "tbl_endereco") %}
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
    id_logradouro,
    codigo,
    numero,
    complemento,
    referencia,
    latitude,
    longitude,
    unidade,
    ativo,
    legado_id,
    legado_sistema,
    codigo_sap,
    viabilidade_autorizada,
    viabilidade_usuario,
    TRY_CAST(viabilidade_data AS TIMESTAMP) AS viabilidade_data,
    viabilidade_justificativa,
    logradouro_tipo,
    logradouro,
    bairro,
    cep,
    id_cidade,
    integracao_status_estoque,
    certificado,
    verificado,
    ftta,
    id_condominio,
    google_maps_resposta,
    google_maps_status

  FROM latest
)

SELECT *
FROM transformed
