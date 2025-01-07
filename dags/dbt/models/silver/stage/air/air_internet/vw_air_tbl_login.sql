{% set catalog_schema_table=source("air_internet", "tbl_login") %}
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
    contrato_codigo,
    contrato_cancelado,
    contrato_unidade,
    contrato_cidade,
    contrato_bairro,
    contrato_pacote_codigo,
    contrato_pacote_nome,
    id_banda,
    endereco_ip,
    usuario,
    senha,
    rota,
    habilitado,
    integracao_status,
    integracao_mensagem,
    wifi_nome,
    wifi_senha,
    onu_serial,
    integracao_transacao,
    cliente_nome,
    onu_modelo,
    olt_ip,
    slot,
    pon,
    roteador_serial,
    sistema_origem,
    ip_radius,
    ip_concentrador,
    possui_circuit_id

  FROM latest
)

SELECT *
FROM transformed
