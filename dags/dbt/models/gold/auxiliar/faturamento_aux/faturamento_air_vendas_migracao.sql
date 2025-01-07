SELECT a.id,
a.id_campanha,
a.id_contrato,
a.data_criacao,
a.data_alteracao,
a.data_venda,
a.natureza
FROM {{ ref('vw_air_tbl_contrato_migracao') }} b
LEFT JOIN {{ ref('vw_air_tbl_venda') }} a
  ON a.id = b.id_venda