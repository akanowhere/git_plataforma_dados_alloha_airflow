SELECT
  a.contrato_air,
  a.mes_referencia,
  a.status_contrato,
  a.segmento,
  a.id_cliente_air,
  a.tipo_pessoa,
  a.polo,
  a.marca,
  a.cidade,
  a.UF,
  a.Regiao,
  a.regional,
  a.sub_regional,
  a.codigo_fatura_sydle,
  a.data_vencimento,
  a.status_fatura,
  a.classificacao_fatura,
  a.valor_fatura,
  a.aging,
  a.classificacao_pdd,
  a.classificacao_car,
  a.forma_pagamento,
  a.data_extracao,
  --------PADRONIZANDO A BASE------------
  CASE
    WHEN MARCA IS NULL THEN 'S/INFORMAÇÃO'
    ELSE MARCA
  END MARCA1,
  CASE
    WHEN segmento IS NULL THEN 'S/INFORMAÇÃO'
    ELSE MARCA
  END SEGMENTO1,
  CASE
    WHEN Regiao IS NULL THEN 'S/INFORMAÇÃO'
    ELSE Regiao
  END REGIAO1,
  CASE
    WHEN regional IS NULL THEN 'S/INFORMAÇÃO'
    ELSE regional
  END REGIONAL1,
  CASE
    WHEN sub_regional IS NULL THEN 'S/INFORMAÇÃO'
    ELSE sub_regional
  END SUB_REGIONAL1,
  ----------Regra de Negócio
  CASE
    WHEN MARCA = 'SUMICITY' AND TIPO_PESSOA = 'FISICO' AND classificacao_pdd IN ('PDD', 'PDD ARRASTO') THEN 'CONTA_PDD'
    WHEN MARCA <> 'SUMICITY' AND classificacao_pdd IN ('PDD', 'PDD ARRASTO') THEN 'CONTA_PDD'
    WHEN MARCA is null AND classificacao_pdd IN ('PDD', 'PDD ARRASTO') THEN 'CONTA_PDD'
    ELSE 'NAO_CONTA_PDD'
  END ENTRA_CALCULO_PDD,
  ------criando indices
  CASE
    WHEN classificacao_car = 'A VENCER' THEN 1
    WHEN classificacao_car = 'DE 1 A 30 DIAS' THEN 2
    WHEN classificacao_car = 'DE 31 A 60 DIAS' THEN 3
    WHEN classificacao_car = 'DE 61 A 90 DIAS' THEN 4
    WHEN classificacao_car = 'DE 91 A 120 DIAS' THEN 5
    WHEN classificacao_car = 'DE 121 A 150 DIAS' THEN 6
    WHEN classificacao_car = 'DE 151 A 180 DIAS' THEN 7
    WHEN classificacao_car = 'DE 181 A 210 DIAS' THEN 8
    WHEN classificacao_car = 'DE 211 A 240 DIAS' THEN 9
    WHEN classificacao_car = 'DE 241 A 270 DIAS' THEN 10
    WHEN classificacao_car = 'DE 271 A 300 DIAS' THEN 11
    WHEN classificacao_car = 'DE 301 A 330 DIAS' THEN 12
    WHEN classificacao_car = 'DE 331 A 365 DIAS' THEN 12
    WHEN classificacao_car = 'MAIOR QUE 365 DIAS' THEN 14
    ELSE 0
  END indice_CAR,
  CASE
    WHEN CLASSIFICACAO_pdd = 'A VENCER' THEN 1
    WHEN CLASSIFICACAO_pdd = 'VENCIDO DE 1 A 90' THEN 2
    WHEN CLASSIFICACAO_pdd = 'PDD' THEN 3
    WHEN CLASSIFICACAO_pdd = 'PDD ARRASTO' THEN 4
    ELSE 0
  END Indice_pdd,
  CONCAT(REGIAO, REGIONAL, SUB_REGIONAL) CHAVE
FROM
  {{ get_catalogo('gold') }}.arrecadacao.pdd_car a