SELECT 
chamada_cobrar,
cliente_nome,
contrato_codigo,
credito_dia,
credito_valor,
data_alteracao,
data_criacao,
excluido,
habilitado,
id,
id_tarifacao,
inbound_calls_limit,
mensagem_integracao,
outbound_calls_limit,
status_integracao,
unidade_sigla,
usuario_alteracao,
usuario_criacao,
vsc_ctrl_number,
vsc_senha

FROM {{ ref('vw_air_tbl_reserva') }}