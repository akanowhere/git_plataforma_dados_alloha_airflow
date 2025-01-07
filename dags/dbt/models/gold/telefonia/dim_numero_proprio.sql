select 

data_alteracao,
data_criacao,
disponivel,
excluido,
id,
id_faixa,
id_reserva,
numero,
portado,
quarentena,
usuario_alteracao,
usuario_criacao

FROM {{ ref('vw_air_tbl_numero_proprio') }}