SELECT DISTINCT 
    try_cast(FO.data_conclusao AS DATE) AS Data,
    FO.codigo_os,
    CASE
        WHEN FO.FILA LIKE 'UPGRADE NÃO LÓGICO' THEN 'MUDANÇA DE ENDEREÇO'
        WHEN FO.fila LIKE 'MUDANÇA DE CÔMODO' THEN 'MUDANÇA DE ENDEREÇO'
        WHEN FO.fila LIKE '%REPARO%' THEN 'REPARO'
        ELSE FO.fila
    END AS Fila,
    REPLACE(DU.regional, 'R', 'T') AS Regional,
    UPPER(DU.sigla) AS Unidade,
    DU.nome AS Cidade,
    UPPER(DU.marca) AS Marca,
    REPLACE(REPLACE(DU.cluster, 'CLUSTER_', ''), 'R', 'T') AS Cluster,
    XA_CUS_EVA_RAT Nota,
    XA_CUS_EVA_NOT Avaliacao
FROM
{{ get_catalogo('gold') }}.ofs.fato_activity OFS
LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_ordem FO ON try_cast(FO.codigo_os AS STRING) = OFS.apptNumber
LEFT JOIN (
SELECT
    A.id_contrato,
    MAX(A.id_endereco) AS id_endereco
FROM
    {{ get_catalogo('gold') }}.base.dim_contrato_entrega A
GROUP BY
    A.id_contrato
) AS CE ON CE.id_contrato = FO.codigo_contrato
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade DU ON DU.sigla = FO.unidade
WHERE
XA_CUS_EVA_RAT IS NOT NULL

ORDER BY 1 DESC

