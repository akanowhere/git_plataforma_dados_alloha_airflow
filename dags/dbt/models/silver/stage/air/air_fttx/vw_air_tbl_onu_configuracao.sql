{% set catalog_schema_table=source("air_fttx", "tbl_onu_configuracao") %}
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
    id_onu,
    contrato_codigo,
    cliente_nome,
    pppoe_login,
    pppoe_senha,
    vsc_numero,
    vsc_senha,
    wifi_ssid,
    wifi_senha,
    vigente,
    possui_tv,
    possui_tel,
    possui_wifi,
    possui_internet,
    unidade_sigla,
    status_integracao,
    mensagem_integracao,
    log_servidor,
    tentativa_configuracao,
    rx_power,
    tx_power,
    id_tecnico,
    nome_tecnico,
    id_terceirizada,
    nome_terceirizada,
    xml_configuracao,
    codigo_integracao_ot,
    id_os,
    dual_box,
    serial_roteador,
    log_roteador,
    motivo_provisionamento

  FROM latest
)

SELECT *
FROM transformed
