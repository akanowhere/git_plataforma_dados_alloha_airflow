{{
  config(
    alias = 'dim_onu_configuracao'
    )
}}

WITH vw_air_tbl_onu_configuracao AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_onu_configuracao') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
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
  motivo_provisionamento,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM vw_air_tbl_onu_configuracao
