SELECT a.id AS id_campanha,
a.nome AS nome_campanha,
c.id AS id_contrato,
c.pacote_codigo,
c.pacote_nome
FROM {{ ref('vw_air_tbl_campanha') }} a
LEFT JOIN {{ ref('vw_air_tbl_contrato_campanha') }} b
	ON b.id_campanha = a.id
LEFT JOIN {{ ref('vw_air_tbl_contrato') }} c
	ON c.id = b.id_contrato
WHERE ((UPPER(a.nome) LIKE '%PERM%') OR (UPPER(a.nome) LIKE '%EXEC%') OR (UPPER(a.nome) LIKE '%COLAB%'))