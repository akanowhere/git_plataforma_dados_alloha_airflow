SELECT DISTINCT
a.id_cliente,
a.Nome_Cliente,
a.categoria_senha
FROM 
(
    SELECT DISTINCT
    clientegold.id_cliente,
    clientesilver.nome Nome_Cliente,
    CASE 
        WHEN instr(clientesilver.senha_sac, regexp_replace(clientegold.cpf, '[.-]', '')) > 0 THEN 'Senha Fraca'
        ELSE 'Senha Forte'
    END AS categoria_senha,
    clientegold.segmento
    
FROM {{ get_catalogo('gold') }}.base.fato_base_ativos_analitica baseativo

LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato contrato
  ON contrato.id_contrato_air = baseativo.id_contrato

LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_cliente clientesilver
  ON clientesilver.id = contrato.id_cliente

LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente clientegold
  ON clientegold.id_cliente = contrato.id_cliente

WHERE
  clientegold.segmento <> 'B2B'
) a

WHERE A.categoria_senha = 'Senha Fraca'

