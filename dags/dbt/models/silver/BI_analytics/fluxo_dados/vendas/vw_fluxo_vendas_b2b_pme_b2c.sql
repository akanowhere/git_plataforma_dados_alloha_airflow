WITH vendas_filtradas AS (
  SELECT
    t1.data_venda,
    t1.data_criacao,
    t1.hora_criacao,
    t1.id_contrato,
    t1.id_contrato_ixc,
    t1.id_contrato_air,
    t1.valor_referencia_b2b,
    t1.id_cliente,
    t1.segmento,
    t1.equipe,
    t1.canal,
    t1.canal_tratado,
    t1.tipo_canal,
    t1.id_vendedor,
    t1.nome_vendedor,
    t1.email_vendedor,
    t1.marca,
    t1.estado,
    t1.cidade,
    t1.bairro,
    t1.cep,
    CAST(t1.latitude AS DOUBLE) AS latitude,
    CAST(t1.longitude AS DOUBLE) AS longitude,
    t1.regional,
    t1.cluster,
    t1.pacote_nome,
    t1.campanha_nome,
    t1.valor_final,
    t1.velocidade
  FROM
    {{ get_catalogo('gold') }}.venda.fato_venda t1
  WHERE
    t1.data_venda >= '2023-06-01'
),
cliente AS (
  SELECT
    t4.id_cliente,
    t4.nome,
    COALESCE(t4.cpf, t4.cnpj) AS cpf_cnpj
  FROM
    {{ get_catalogo('gold') }}.base.dim_cliente t4
  WHERE
    t4.fonte = 'AIR'
),
vendas_gerais AS (
  SELECT
    t6.id_contrato,
    t6.id_endereco
  FROM
    {{ get_catalogo('gold') }}.venda.dim_venda_geral t6
  WHERE
    t6.natureza = 'VN_NOVO_CONTRATO'
),
endereco AS (
  SELECT
    t7.id_endereco,
    t7.fonte,
    t7.logradouro,
    t7.numero
  FROM
    {{ get_catalogo('gold') }}.base.dim_endereco t7
  WHERE
    t7.fonte = 'AIR'
),
contrato AS (
  SELECT
    t5.id_contrato,
    t5.data_primeira_ativacao,
    t5.data_cancelamento,
    t5.status,
    t5.nome_suspensao
  FROM
    {{ get_catalogo('gold') }}.base.dim_contrato t5
)
SELECT
  vf.data_venda,
  vf.data_criacao,
  vf.hora_criacao,
  c.data_primeira_ativacao AS data_ativacao,
  c.data_cancelamento,
  vf.id_contrato,
  c.status AS status_contrato,
  vf.id_contrato_ixc,
  vf.id_contrato_air,
  vf.valor_referencia_b2b,
  vf.id_cliente,
  cl.nome,
  vf.segmento,
  c.nome_suspensao,
  vf.equipe,
  cl.cpf_cnpj,
  vf.canal,
  vf.canal_tratado,
  vf.tipo_canal,
  vf.id_vendedor,
  vf.nome_vendedor,
  vf.email_vendedor,
  vf.marca,
  vf.estado,
  vf.cidade,
  vf.bairro,
  vf.cep,
  e.logradouro,
  e.numero,
  vf.latitude,
  vf.longitude,
  vf.regional,
  vf.cluster,
  vf.pacote_nome,
  vf.campanha_nome,
  vf.valor_final,
  vf.velocidade
FROM
  vendas_filtradas vf
  LEFT JOIN cliente cl 
    ON cl.id_cliente = vf.id_cliente
  LEFT JOIN contrato c 
    ON c.id_contrato = vf.id_contrato_air
  LEFT JOIN vendas_gerais vg 
    ON vg.id_contrato = vf.id_contrato_air
  LEFT JOIN endereco e 
    ON e.id_endereco = vg.id_endereco