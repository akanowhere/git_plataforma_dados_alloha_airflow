WITH tbl_login AS (
  SELECT *
  FROM {{ ref("vw_air_tbl_login") }}
),

tbl_banda AS (
  SELECT *
  FROM {{ ref("vw_air_tbl_banda") }}
),

tbl_onu_modelo AS (
  SELECT *
  FROM {{ ref("vw_air_tbl_onu_modelo") }}
),

joined AS (
  SELECT
    l.id AS id_login,
    l.data_criacao AS data_criacao,
    l.data_alteracao AS data_alteracao,
    l.usuario_criacao,
    l.usuario_alteracao,
    l.excluido,
    l.contrato_codigo,
    l.contrato_cancelado,
    l.contrato_unidade,
    l.contrato_cidade,
    l.contrato_bairro,
    l.contrato_pacote_codigo,
    l.contrato_pacote_nome,
    l.id_banda,
    l.endereco_ip,
    l.usuario,
    l.senha,
    l.rota,
    l.habilitado,
    l.integracao_status,
    l.integracao_mensagem,
    l.wifi_nome,
    l.wifi_senha,
    l.onu_serial,
    l.integracao_transacao,
    l.cliente_nome,
    l.onu_modelo,
    l.olt_ip,
    l.slot,
    l.pon,
    l.roteador_serial,
    l.possui_circuit_id,
    b.data_criacao AS data_criacao_banda,
    b.usuario_criacao AS usuario_criacao_banda,
    b.data_alteracao AS data_alteracao_banda,
    b.usuario_alteracao AS usuario_alteracao_banda,
    b.excluido AS excluido_banda,
    b.codigo_radius AS codigo_radius_banda,
    b.descricao AS descricao_banda,
    b.mikrotik AS mikrotik_banda,
    b.juniper_in AS juniper_in_banda,
    b.juniper_out AS juniper_out_banda,
    b.juniper_in_v6 AS juniper_in_v6_banda,
    b.juniper_out_v6 AS juniper_out_v6_banda,
    b.ativo AS ativo_banda,
    o.id AS id_onu,
    o.data_criacao AS data_criacao_onu,
    o.usuario_criacao AS usuario_criacao_onu,
    o.data_alteracao AS data_alteracao_onu,
    o.usuario_alteracao AS usuario_alteracao_onu,
    o.excluido AS excluido_onu,
    o.nome AS nome_onu,
    o.fabricante AS fabricante_onu,
    o.qtd_portas_lan AS qtd_portas_lan_onu,
    o.qtd_portas_telefonia AS qtd_portas_telefonia_onu,
    o.possui_wifi AS possui_wifi_onu,
    o.porta_giga AS porta_giga_onu,
    o.wifi AS wifi_onu,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM tbl_login AS l
  LEFT JOIN tbl_banda AS b ON l.id_banda = b.id
  LEFT JOIN tbl_onu_modelo AS o ON l.onu_modelo = o.nome AND o.excluido = FALSE
)

SELECT *
FROM joined
