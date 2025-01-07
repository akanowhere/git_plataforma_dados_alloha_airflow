select 

data_alteracao,
data_criacao,
data_portabilidade,
devolvido,
excluido,
id,
id_arealocal,
id_reserva,
motivo,
numero,
operadora,
usuario_alteracao,
usuario_criacao



FROM {{ ref('vw_air_tbl_numero_portado') }}