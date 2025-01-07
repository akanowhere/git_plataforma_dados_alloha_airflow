WITH nfs AS (
SELECT  NotaFiscal.id_nota_fiscal,
         fatura_id_sydle,
         numero,
         cliente_id_sydle,
         cliente_nome,
         cliente_documento,
        CASE
            WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
            THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
            ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
        END AS mes_referencia,
         status,
         tipo,
         data_emissao_nota_fiscal,
         CAST(REPLACE(REPLACE(UPPER({{ translate_column('tomador_endereco_cidade') }}), CHAR(13), ''), CHAR(10), '') AS VARCHAR(255)) as Cidade,
		 Case UPPER({{ translate_column('tomador_endereco_estado') }})
					When 'ACRE' Then 'AC'			When 'ALAGOAS' Then 'AL'			When 'AMAPA' Then 'AP'			When 'AMAZONAS' Then 'AM'
					When 'BAHIA' Then 'BA'			When 'CEARA' Then 'CE'				When 'DISTRITO FEDERAL' then 'DF'			When 'ESPIRITO SANTO' Then 'ES'
					When 'GOIAS' Then 'GO'			When 'MARANHAO' Then 'MA'			When 'MATO GROSSO' Then 'MT'			When 'MATO GROSSO DO SUL' Then 'MS'
					When 'MINAS GERAIS' Then 'MG'			When 'PARA' Then 'PA'			When 'PARAIBA' Then 'PB'			When 'PARANA' Then 'PR'
					When 'PERNAMBUCO' Then 'PE'			When 'PIAUI' Then 'PI'			When 'RIO DE JANEIRO' Then 'RJ'			When 'RIO GRANDE DO NORTE' Then 'RN'
					When 'RIO GRANDE DO SUL' Then 'RS'			When 'RONDONIA' Then 'RO'			When 'RORAIMA' Then 'RR'			When 'SANTA CATARINA' Then 'SC'
					When 'SAO PAULO' Then 'SP'			When 'SERGIPE' Then 'SE'			When 'TOCANTINS' Then 'TO'  End As Estado,
		Cast(NotaFiscal.valor_total_nf as decimal(38,2)) valor_total_nf

FROM {{ ref('dim_nota_fiscal') }} as NotaFiscal
WHERE
  (( MONTH(NotaFiscal.data_emissao_nota_fiscal) = MONTH(DATEADD(DAY, -1, current_date()))
  And YEAR(NotaFiscal.data_emissao_nota_fiscal) = YEAR(DATEADD(DAY, -1, current_date()))
  AND UPPER({{ translate_column('NotaFiscal.tipo') }}) in ('COMUNICACAO',
                          'NOTA FATURA',
                          'TELECOMUNICACOES'))
  OR
  (MONTH(NotaFiscal.data_emissao_recibo) = MONTH(DATEADD(DAY, -1, current_date()))
  And YEAR(NotaFiscal.data_emissao_recibo) = YEAR(DATEADD(DAY, -1, current_date()))
  AND UPPER({{ translate_column('NotaFiscal.tipo') }}) = 'NOTA FISCAL DE SERVICO (NFS-E)'))
),

nfs_com_produtos AS (
  SELECT NotaFiscal.*,
  	Produto.nome_produto,
	Produto.descricao_produto,
	Produto.servico_tributavel_nome,
    Cast(Cast(Produto.valor_total_dos_impostos_do_produto as Float) as  NUMERIC(15, 2)) valor_total_impostos_do_produto,
    Cast(Cast(Produto.valor_base_produto as Float) as  NUMERIC(15, 2)) valor_base_produto

  FROM nfs NotaFiscal
  Left Join {{ ref('dim_nota_fiscal_produtos') }} as Produto
	ON Produto.id_nota_fiscal = NotaFiscal.id_nota_fiscal
),

endereco_contrato AS (
  SELECT E.estado,
        E.cidade,
        E.unidade,
        A.id_contrato_air AS CODIGO_CONTRATO_AIR
  FROM {{ ref('dim_contrato') }} as A
  INNER JOIN
    (SELECT MAX(id_endereco) AS endereco,
            id_contrato
    FROM {{ ref('dim_contrato_entrega') }} as dim_contrato_entrega
    GROUP BY id_contrato) AS tb_temp ON tb_temp.id_contrato = A.id_contrato_air
  INNER JOIN {{ ref('dim_endereco') }} as E ON E.id_endereco = tb_temp.endereco
),

nfs_dados_faturas AS (
SELECT a.id_cliente AS id_cliente_AIR,
        A.id_contrato_air as CODIGO_CONTRATO_AIR,
        a.status,
		UPPER(U.marca) AS marca,
    CAST(UPPER({{ translate_column('endereco_contrato.cidade') }}) AS VARCHAR(255)) As Cidade,
		UPPER(endereco_contrato.estado) AS estado,
		UPPER(subregionais.macro_regional) AS Regiao,
    UPPER(subregionais.sigla_regional) AS regional,
    UPPER(subregionais.subregional) AS sub_regional,
    cast(COALESCE(endereco_contrato.unidade, A.unidade_atendimento) AS varchar(16)) AS unidade_atendimento,
		CASE
        WHEN A.b2b = TRUE THEN 'B2B'
        WHEN A.pme = TRUE THEN 'PME'
        ELSE 'B2C'
    END AS segmento,
    c.id_fatura,
    a.fechamento_vencimento AS ciclo,
    c.mes_referencia,
    c.data_vencimento,
    c.data_pagamento,
    c.valor_pago,
    c.contrato_air,
		c.codigo_fatura_sydle,
		c.classificacao  as Classificacao_Fatura,
		a.dia_vencimento

FROM {{ ref('dim_contrato') }} as  A
LEFT JOIN {{ ref('dim_faturas_mailing') }} as C ON A.id_contrato_air = C.contrato_air
  AND UPPER({{ translate_column('C.classificacao') }}) <> 'NEGOCIACAO'
LEFT JOIN endereco_contrato
	ON endereco_contrato.CODIGO_CONTRATO_AIR = A.id_contrato_air
LEFT JOIN {{ ref('dim_unidade') }} as U ON U.sigla = COALESCE(endereco_contrato.unidade, A.unidade_atendimento)
AND U.fonte = 'AIR'
LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais as subregionais
    ON {{ translate_column('endereco_contrato.cidade') }} = subregionais.cidade_sem_acento
    AND UPPER(endereco_contrato.estado) = subregionais.uf
WHERE c.id_fatura in (select distinct fatura_id_sydle from nfs_com_produtos )
),

nfs_e_faturas AS (
SELECT
  CASE
    WHEN UPPER(marca) like 'SUMICITY' THEN 'Polo Sumicity'
    WHEN UPPER(marca) like 'CLICK' THEN 'Polo Sumicity'
    WHEN UPPER(marca) like '%GIGA%' THEN 'Polo Sumicity'
    WHEN UPPER(marca) like '%UNIVOX%' THEN 'Polo Sumicity'
    WHEN UPPER(marca) like '%VIP%' THEN 'Polo VIP'
    WHEN UPPER(marca) like '%NIU%' THEN 'Polo VIP'
    WHEN UPPER(marca) like '%PAMNET%' THEN 'Polo VIP'
    WHEN UPPER(marca) like '%LIGUE%' THEN 'Polo VIP'
    WHEN UPPER(marca) like '%MOB%' THEN 'Polo Mob'
  ELSE NULL
  END AS polo,
  fatura.marca,
  fatura.unidade_atendimento as Unidade,
  fatura.Regional,
  NotaFiscal.numero,
  NotaFiscal.nome_produto,
  NotaFiscal.servico_tributavel_nome,
  fatura.Segmento,
  NotaFiscal.mes_referencia,
  fatura.data_vencimento,
  fatura.data_pagamento,
  fatura.valor_pago,
  fatura.ciclo,
  NotaFiscal.Cidade,
  NotaFiscal.Estado,
  NotaFiscal.cliente_nome,
  NotaFiscal.cliente_documento,
  NotaFiscal.tipo,
  NotaFiscal.descricao_produto,
  NotaFiscal.fatura_id_sydle id_fatura,
  NotaFiscal.id_nota_fiscal,
  Fatura.contrato_air,
  NotaFiscal.data_emissao_nota_fiscal,
  NotaFiscal.valor_total_nf,
  NotaFiscal.status,
  Fatura.Classificacao_Fatura,
  Cast(NotaFiscal.valor_total_impostos_do_produto  as decimal(38,2)) AS valor_total_dos_impostos_do_produto, --valor equivalente do imposto pelo periodo de utilização
  Cast(NotaFiscal.valor_base_produto as decimal(38,2)) as Valor_Base_Produto, --valor proporcional da nf
  fatura.dia_vencimento,
  fatura.Regiao,
  fatura.sub_regional

FROM nfs_com_produtos AS NotaFiscal
left join nfs_dados_faturas AS fatura on NotaFiscal.fatura_id_sydle = fatura.id_fatura
),

impostosbase AS (
Select id_nota_fiscal
      ,descricao_produto
      ,imposto
	  ,aliquota
      ,valor
From {{ ref('dim_nota_fiscal_impostos') }} as dim_nota_fiscal_impostos --impostos da not
where id_nota_fiscal in (select distinct id_nota_fiscal from  nfs_com_produtos)
),

Impostos AS (
    SELECT id_nota_fiscal
    ,descricao_produto
    ,Ltrim(rtrim(Case When descricao_produto like '%/%' Then substring(descricao_produto, 25,100) Else descricao_produto End )) as  produto
    ,Case When UPPER(imposto) = 'ICMS' Then valor Else 0 End As ICMS
    , Case When UPPER(imposto) = 'ICMS' Then try_cast(aliquota as decimal(24,4)) Else 0 End As P_ICMS
    , Case When UPPER(imposto) = 'PIS' Then valor Else 0 End As PIS
    , Case When UPPER(imposto) = 'PIS' Then try_cast(aliquota as decimal(24,4)) Else 0 End As P_PIS
    , Case When UPPER(imposto) = 'COFINS' Then valor Else 0 End As COFINS
    , Case When UPPER(imposto) = 'COFINS' Then try_cast(aliquota as decimal(24,4)) Else 0 End As P_COFINS
    , Case When UPPER(imposto) = 'ISS' Then valor Else 0 End As ISS
    , Case When UPPER(imposto) = 'ISS' Then try_cast(aliquota as decimal(24,4)) Else 0 End As P_ISS
    From impostosbase
),

Soma_Impostos AS (
Select id_nota_fiscal,
	   produto,
	   descricao_produto,
	   Sum(ICMS) ICMS,
	   SUM(P_ICMS) P_ICMS,
	   SUM(PIS) PIS,
	   SUM(P_PIS) P_PIS,
	   SUM(COFINS) COFINS,
	   SUM(P_COFINS) P_COFINS,
	   SUM(ISS) ISS,
	   SUM(P_ISS) P_ISS
From Impostos
Group By id_nota_fiscal,
	   produto,
	   descricao_produto
)

SELECT Polo
      ,Marca
      ,unidade
      ,Regional
      ,Regiao
      ,sub_regional
      ,numero
      ,nome_produto
      ,servico_tributavel_nome
      ,segmento
      ,mes_referencia
      ,data_vencimento
      ,data_pagamento
      ,valor_pago
      ,Ciclo
      ,Cidade
      ,Estado
      ,cliente_nome
      ,cliente_documento
      ,Soma_Impostos.ICMS AS ICMS
      ,Soma_Impostos.P_ICMS AS P_ICMS
      ,Soma_Impostos.PIS AS PIS
      ,Soma_Impostos.P_PIS AS P_PIS
      ,Soma_Impostos.COFINS AS COFINS
      ,Soma_Impostos.P_COFINS AS P_COFINS
      ,tipo
      ,Soma_Impostos.descricao_produto
      ,id_fatura
      ,nfs_e_faturas.id_nota_fiscal
      ,contrato_air
      ,data_emissao_nota_fiscal
      ,valor_total_nf
      ,status
	    ,Classificacao_Fatura
      ,valor_total_dos_impostos_do_produto
      ,valor_base_produto
      ,dia_vencimento
      ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM nfs_e_faturas
LEFT JOIN Soma_Impostos ON nfs_e_faturas.id_nota_fiscal = Soma_Impostos.id_nota_fiscal
and nfs_e_faturas.descricao_produto = Soma_Impostos.descricao_produto

UNION ALL

SELECT Polo
      ,Marca
      ,unidade
      ,Regional
      ,Regiao
      ,sub_regional
      ,numero
      ,nome_produto
      ,servico_tributavel_nome
      ,segmento
      ,mes_referencia
      ,data_vencimento
      ,data_pagamento
      ,valor_pago
      ,Ciclo
      ,Cidade
      ,Estado
      ,cliente_nome
      ,cliente_documento
      ,ICMS
      ,P_ICMS
      ,PIS
      ,P_PIS
      ,COFINS
      ,P_COFINS
      ,tipo
      ,descricao_produto
      ,id_fatura
      ,id_nota_fiscal
      ,contrato_air
      ,data_emissao_nota_fiscal
      ,valor_total_nf
      ,status
	    ,Classificacao_Fatura
      ,valor_total_dos_impostos_do_produto
      ,valor_base_produto
      ,dia_vencimento
      ,data_extracao
FROM {{ this }}
WHERE mes_referencia <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
