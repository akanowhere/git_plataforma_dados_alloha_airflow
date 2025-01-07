SELECT 
id AS id_contrato_entrega
,TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao
,TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao
,usuario_criacao
,usuario_alteracao
,excluido
,codigo
,id_contrato
,id_endereco
,instalado
,TRY_CAST(data_ativacao AS TIMESTAMP) AS data_ativacao
,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS  data_extracao
FROM {{ ref("vw_air_tbl_contrato_entrega") }}
