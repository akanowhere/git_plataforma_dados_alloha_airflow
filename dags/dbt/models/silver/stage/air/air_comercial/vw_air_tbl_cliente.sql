{% set catalog_schema_table=source("air_comercial", "tbl_cliente") %}
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
    tipo,
    nome,
    tratamento,
    grupo,
    codigo,
    senha_sac,
    fonte,
    legado_id,
    legado_sistema,
    integracao_status,
    integracao_mensagem,
    integracao_codigo,
    integracao_transacao,
    marcador,
    cliente_b2b,
    integracao_status_sydle,
    integracao_mensagem_sydle,
    integracao_transacao_sydle,
    id_suporte_sumicity_virtual_um,
    id_suporte_sumicity_virtual_dois,
    id_oauth,
    id_oauth_giga_mais,
    id_oauth_click,
    cupom,
    id_oauth_univox,
    id_oauth_ligue

  FROM latest
)

SELECT *
FROM transformed
