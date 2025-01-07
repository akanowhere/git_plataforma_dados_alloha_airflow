SELECT
  DISTINCT v.data_venda,
  v.data_criacao,
  v.hora_criacao,
  v.id_contrato,
  v.id_contrato_air,
  v.id_contrato_ixc,
  v.id_cliente,
  v.segmento,
  v.equipe,
  CASE WHEN v.equipe = 'EQUIPE_TLV_ATIVO' THEN 'ATIVO' ELSE v.canal END canal,
  CASE WHEN v.equipe = 'EQUIPE_TLV_ATIVO' THEN 'ATIVO' ELSE v.canal_tratado END canal_tratado,
  v.tipo_canal,
  v.id_vendedor,
  v.nome_vendedor,
  v.unidade_atendimento,
  v.unidade,
  v.fonte,
  v.estado,
  v.cidade,
  v.bairro,
  COALESCE(sr.sigla_regional,REPLACE(REPLACE(r.regional, 'EGIAO_0', ''), 'EGIAO-0', ''), v.regional) regional,
  r.marca,
  v.pacote_nome,
  v.velocidade,
  v.campanha_nome,
  v.produto,
  v.pacote_tv,
  v.cobranca,
  v.valor_total,
  v.pacote_valor_total,
  v.desconto,
  v.valor_final,
  v.afiliado_assine,
  v.email_vendedor,
  v.cupom,
  v.cupom_valido,
  COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional,
  v.duracao_desconto,
  ROUND(((v.valor_final * COALESCE(v.duracao_desconto,0)) + v.valor_total * (12 - COALESCE(v.duracao_desconto,0)))/12,2) as ticket_medio_ponderado
FROM
  gold.venda.fato_venda v
  LEFT JOIN gold.base.dim_unidade r ON v.unidade = r.sigla
  LEFT JOIN silver.stage_seeds_data.subregionais sr ON 
  UPPER(
      TRANSLATE(
        v.cidade,
        "ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ",
        "AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao"
      )
    ) = UPPER(sr.cidade_sem_acento)
  AND v.estado = sr.uf
WHERE
  data_venda >= '2023-01-01';