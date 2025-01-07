SELECT
  DISTINCT os.id AS id_os,
  os.codigo_cliente AS id_cliente,
  os.contrato_codigo AS id_contrato,
  os.data_criacao,
  os.usuario_criacao,
  os.data_alteracao,
  os.data_conclusao,
  os.usuario_alteracao,
  os.excluido,
  os.id_agenda,
  os.id_chamado,
  t.id_terceirizada,
  tc.nome AS nome_terceirizada,
  os.agendamento_data,
  os.turno,
  os.equipe,
  os.classificacao,
  os.servico,
  os.id_tecnico,
  t.nome AS nome_tecnico,
  os.pacote,
  os.unidade,
  os.codigo_fila,
  os.status,
  os.motivo_conclusao,
  f.nome AS nome_fila
FROM
  {{ get_catalogo('gold') }}.chamados.dim_os os
   LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_os_tecnico t 
          ON t.id = os.id_tecnico
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
          ON os.unidade = u.sigla
         AND u.fonte = 'AIR'
   LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_os_terceirizada tc
          ON t.id_terceirizada = tc.id
   LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_fila f
          ON os.codigo_fila = f.codigo
WHERE
  os.data_conclusao IS NULL