
SELECT id AS id_contrato_reajuste
    ,id_contrato
    ,cast(valor_anterior AS DECIMAL(10,2)) AS valor_anterior
    ,valor_indice as porcentagem_reajuste
	,cast(valor_final AS DECIMAL(10,2)) AS valor_final
    ,versao_contrato
    ,cast(data_primeira_consulta AS TIMESTAMP) AS data_primeira_consulta
    ,cast(data_segunda_consulta AS TIMESTAMP) AS data_segunda_consulta
    ,cast(data_processamento AS TIMESTAMP) AS data_processamento
    ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS  data_extracao
FROM {{ ref("vw_air_tbl_contrato_reajuste") }}
