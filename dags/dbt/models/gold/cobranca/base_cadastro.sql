WITH login AS (
    SELECT
    login.contrato_codigo,
    login.usuario
    FROM {{ ref('vw_air_tbl_login') }} login
    INNER JOIN (
        SELECT
        max(login.id) AS id
        FROM {{ ref('vw_air_tbl_login') }} login
        INNER JOIN {{ ref('vw_air_tbl_contrato') }} ctr ON ctr.id = login.contrato_codigo
        WHERE
        login.excluido = 0
        GROUP BY
        login.contrato_codigo
    ) AS A ON A.id = login.id
),

cobranca_dados_cadastros AS (
	SELECT DISTINCT dim_contrato.id_cliente AS id_cliente_air,
	dim_contrato.id_contrato_air AS contrato_air,
	dim_contrato.status as status_contrato,
	dim_contrato.data_criacao AS data_abertura_cadastro,
	dim_contrato.data_primeira_ativacao as ativacao_contrato,
	dim_contrato.data_cancelamento as data_cancelamento_contrato,
	dim_contrato.dia_vencimento,
	dim_contrato.fechamento_vencimento,
	login.usuario AS login_contrato,
	COALESCE(dim_cliente.cpf, dim_cliente.cnpj) AS num_cpf_cnpj,
	dim_cliente.nome AS nome_cliente,
	dim_cliente.data_nascimento,
	CASE
		WHEN UPPER(dim_cliente.tipo) = 'FISICO' THEN 'PF'
		WHEN UPPER(dim_cliente.tipo) = 'JURIDICO' THEN 'PJ'
	ELSE NULL END AS tipo_pessoa,
	CASE
		WHEN dim_contrato.b2b = TRUE THEN 'B2B'
		WHEN dim_contrato.pme = TRUE THEN 'PME'
	ELSE 'B2C' END AS tipo_negocio,
	fato_venda.canal_tratado as canal_venda

	FROM {{ ref('dim_contrato') }} as dim_contrato

	LEFT JOIN {{ ref('dim_cliente') }} as dim_cliente
		ON dim_contrato.id_cliente = dim_cliente.id_cliente AND dim_cliente.fonte = 'AIR'

	LEFT JOIN {{ ref('fato_venda') }} as fato_venda
		ON dim_contrato.id_contrato_air = fato_venda.id_contrato_air and fato_venda.fonte = 'AIR'

	LEFT JOIN login
		on dim_contrato.id_contrato_air = login.contrato_codigo
),

cobranca_dados_endereco AS (
	SELECT DISTINCT B.id_contrato_air AS contrato_air,
	CASE
		WHEN UPPER(U.marca) like 'SUMICITY' THEN 'Polo Sumicity'
		WHEN UPPER(U.marca) like 'CLICK' THEN 'Polo Sumicity'
		WHEN UPPER(U.marca) like '%GIGA%' THEN 'Polo Sumicity'
		WHEN UPPER(U.marca) like '%UNIVOX%' THEN 'Polo Sumicity'
		WHEN UPPER(U.marca) like '%VIP%' THEN 'Polo VIP'
		WHEN UPPER(U.marca) like '%NIU%' THEN 'Polo VIP'
		WHEN UPPER(U.marca) like '%PAMNET%' THEN 'Polo VIP'
		WHEN UPPER(U.marca) like '%LIGUE%' THEN 'Polo VIP'
		WHEN UPPER(U.marca) like '%MOB%' THEN 'Polo Mob'
	ELSE NULL
	END AS polo,
	U.marca,
	CASE
		WHEN UPPER(U.regional) = 'REGIAO_06' THEN 'R6'
		WHEN UPPER(U.regional) = 'REGIAO-07' THEN 'R7'
	ELSE U.regional END AS regional,
    UPPER(subregionais.macro_regional) AS Regiao,
    UPPER(subregionais.subregional) AS sub_regional,
	endereco_contrato.cep,
	endereco_contrato.estado,
	UPPER({{ translate_column('endereco_contrato.cidade') }}) as cidade,
	endereco_contrato.latitude,
	endereco_contrato.longitude

	FROM {{ ref('dim_contrato') }} AS B
    LEFT JOIN (
        SELECT
            E.estado,
            E.cidade,
            E.unidade,
            E.cep,
            E.latitude,
            E.longitude,
            A.id_contrato_air as CODIGO_CONTRATO_AIR
        FROM {{ ref('dim_contrato') }} AS  A
        INNER JOIN (
            SELECT
                MAX(A.id_endereco) AS endereco,
                id_contrato
            FROM {{ ref('dim_contrato_entrega') }} AS A
            GROUP BY
                id_contrato
        ) AS tb_temp
        ON tb_temp.id_contrato = A.id_contrato_air
        INNER JOIN {{ ref('dim_endereco') }} as E
        ON E.id_endereco = tb_temp.endereco
    ) AS endereco_contrato
    ON endereco_contrato.CODIGO_CONTRATO_AIR = B.id_contrato_air
    LEFT JOIN {{ ref('dim_unidade') }} AS U
    ON U.sigla = COALESCE(endereco_contrato.unidade, B.unidade_atendimento)
    AND U.fonte = 'AIR'
    LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais
    ON {{ translate_column('endereco_contrato.cidade') }} = subregionais.cidade_sem_acento
    AND UPPER(endereco_contrato.estado) = subregionais.uf
),

cobranca_dados_contatos_air AS (
	SELECT DISTINCT dim_contrato.id_contrato_air AS contrato_air,
	telefone_air_1.telefone_tratado AS tel_1_air,
	telefone_air_2.telefone_tratado AS tel_2_air,
	telefone_air_3.telefone_tratado AS tel_3_air,
	email_air_1.contato AS email_air_1,
	email_air_2.contato AS email_air_2

	FROM {{ ref('dim_contrato') }} AS dim_contrato

	--TELEFONE AIR
	LEFT JOIN (
		SELECT A.*
		FROM (
			SELECT id_cliente, telefone_tratado,
			ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_extracao) as rn
			FROM {{ ref('dim_contato_telefone') }} AS dim_contato_telefone
			WHERE excluido = 0 and telefone_tratado <> '99999999999'
		) A
		WHERE A.rn = 1
	) AS telefone_air_1
		ON telefone_air_1.id_cliente = dim_contrato.id_cliente

	LEFT JOIN (
		SELECT A.*
		FROM (
			SELECT id_cliente, telefone_tratado,
			ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_extracao) as rn
			FROM {{ ref('dim_contato_telefone') }} AS dim_contato_telefone
			WHERE excluido = 0 and telefone_tratado <> '99999999999'
		) A
		WHERE A.rn = 2
	) AS telefone_air_2
		ON telefone_air_2.id_cliente = dim_contrato.id_cliente

	LEFT JOIN (
		SELECT A.*
		FROM (
			SELECT id_cliente, telefone_tratado,
			ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_extracao) as rn
			FROM {{ ref('dim_contato_telefone') }} AS dim_contato_telefone
			WHERE excluido = 0 and telefone_tratado <> '99999999999'
		) A
		WHERE A.rn = 3
	) AS telefone_air_3
		ON telefone_air_3.id_cliente = dim_contrato.id_cliente

	---EMAIL_AIR
	LEFT JOIN (
		SELECT A.*
		FROM (
			SELECT id_cliente, contato,
			ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_extracao) as rn
			FROM {{ ref('dim_contato_email') }} AS dim_contato_email
			WHERE UPPER(origem) = 'AIR' AND UPPER(tipo) = 'EMAIL' AND excluido = 0
			AND NOT(
				UPPER(contato) NOT LIKE '%@%'
				OR UPPER(contato) LIKE '%ATUALIZ%'
				OR UPPER(contato) LIKE '%SAC@SUMICITY%'
				OR UPPER(contato) LIKE '%AUTALIZE%'
				or UPPER(contato) LIKE '%TESTE%'
				OR UPPER(contato) LIKE '%NAOTEM%'
				OR UPPER(contato) LIKE '%N_OCONSTA%'
				OR UPPER(contato) LIKE '%RCA@%'
				OR UPPER(contato) LIKE '%SUMI%'
				OR UPPER(contato) LIKE '%@TEM%')
		) A
		WHERE A.rn = 1
	) AS email_air_1
		ON email_air_1.id_cliente = dim_contrato.id_cliente

	LEFT JOIN (
		SELECT A.*
		FROM (
			SELECT id_cliente, contato,
			ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_extracao) as rn
			FROM {{ ref('dim_contato_email') }} AS dim_contato_email
			WHERE UPPER(origem) = 'AIR' AND UPPER(tipo) = 'EMAIL' AND excluido = 0
			AND NOT(
				UPPER(contato) NOT LIKE '%@%'
				OR UPPER(contato) LIKE '%ATUALIZ%'
				OR UPPER(contato) LIKE '%SAC@SUMICITY%'
				OR UPPER(contato) LIKE '%AUTALIZE%'
				or UPPER(contato) LIKE '%TESTE%'
				OR UPPER(contato) LIKE '%NAOTEM%'
				OR UPPER(contato) LIKE '%N_OCONSTA%'
				OR UPPER(contato) LIKE '%RCA@%'
				OR UPPER(contato) LIKE '%SUMI%'
				OR UPPER(contato) LIKE '%@TEM%')
		) A
		WHERE A.rn = 2
	) AS email_air_2
		ON email_air_2.id_cliente = dim_contrato.id_cliente
),

cobranca_dados_contatos_sydle AS (
SELECT DISTINCT dim_contrato.id_contrato_air AS contrato_air,
telefone_sydle_1.numero AS tel_1_sydle,
telefone_sydle_2.numero AS tel_2_sydle,
telefone_sydle_3.numero AS tel_3_sydle,
email_sydle_1.email AS email_1_sydle,
email_sydle_2.email AS email_2_sydle

FROM {{ ref('dim_contrato') }} AS dim_contrato

--TELEFONE SYDLE
LEFT JOIN (
	SELECT A.*
	FROM (
		SELECT dim_contrato_sydle.codigo_externo,
		dim_cliente_telefones.numero,
		tipo_telefone,
		ROW_NUMBER() OVER (PARTITION BY dim_contrato_sydle.codigo_externo ORDER BY 
		  CASE
			WHEN UPPER(tipo_telefone) = 'PRINCIPAL' then 1 
			WHEN UPPER(tipo_telefone) = 'ATENDIMENTO' then 2
			WHEN UPPER(tipo_telefone) = 'RESIDENCIAL' then 3
			WHEN UPPER(tipo_telefone) = 'COMERCIAL' then 4
			ELSE 5 END ) AS rn

		FROM {{ ref('dim_contrato_sydle') }} AS dim_contrato_sydle
		LEFT JOIN {{ ref('dim_cliente_telefones') }} AS dim_cliente_telefones
			ON dim_contrato_sydle.comprador_id_sydle = dim_cliente_telefones.id_cliente
		where LEN(numero) > 8 and numero <> '99999999999'
	) A
	WHERE A.codigo_externo IS NOT NULL AND A.rn = 1
) AS telefone_sydle_1
	ON telefone_sydle_1.codigo_externo = dim_contrato.id_contrato_air

LEFT JOIN (
	SELECT A.*
	FROM (
		SELECT dim_contrato_sydle.codigo_externo,
		dim_cliente_telefones.numero,
		tipo_telefone,
		ROW_NUMBER() OVER (PARTITION BY dim_contrato_sydle.codigo_externo ORDER BY
		  CASE
			WHEN UPPER(tipo_telefone) = 'PRINCIPAL' then 1
			WHEN UPPER(tipo_telefone) = 'ATENDIMENTO' then 2
			WHEN UPPER(tipo_telefone) = 'RESIDENCIAL' then 3
			WHEN UPPER(tipo_telefone) = 'COMERCIAL' then 4
			ELSE 5 END ) AS rn

		FROM {{ ref('dim_contrato_sydle') }} AS dim_contrato_sydle
		LEFT JOIN {{ ref('dim_cliente_telefones') }} AS dim_cliente_telefones
			ON dim_contrato_sydle.comprador_id_sydle = dim_cliente_telefones.id_cliente
		where LEN(numero) > 8 and numero <> '99999999999'
	) A
	WHERE A.codigo_externo IS NOT NULL AND A.rn = 2
) AS telefone_sydle_2
	ON telefone_sydle_2.codigo_externo = dim_contrato.id_contrato_air

LEFT JOIN (
	SELECT A.*
	FROM (
		SELECT dim_contrato_sydle.codigo_externo,
		dim_cliente_telefones.numero,
		tipo_telefone,
		ROW_NUMBER() OVER (PARTITION BY dim_contrato_sydle.codigo_externo ORDER BY
		  CASE
			WHEN UPPER(tipo_telefone) = 'PRINCIPAL' then 1
			WHEN UPPER(tipo_telefone) = 'ATENDIMENTO' then 2
			WHEN UPPER(tipo_telefone) = 'RESIDENCIAL' then 3
			WHEN UPPER(tipo_telefone) = 'COMERCIAL' then 4
			ELSE 5 END ) AS rn

		FROM {{ ref('dim_contrato_sydle') }} AS dim_contrato_sydle
		LEFT JOIN {{ ref('dim_cliente_telefones') }} AS dim_cliente_telefones
			ON dim_contrato_sydle.comprador_id_sydle = dim_cliente_telefones.id_cliente
		where LEN(numero) > 8 and numero <> '99999999999'
	) A
	WHERE A.codigo_externo IS NOT NULL AND A.rn = 3
) AS telefone_sydle_3
	ON telefone_sydle_3.codigo_externo = dim_contrato.id_contrato_air

---EMAIL SYDLE
LEFT JOIN (
	SELECT A.*
	FROM (
		SELECT dim_contrato_sydle.codigo_externo,
		dim_cliente_emails.email,
		ROW_NUMBER() OVER (PARTITION BY dim_contrato_sydle.codigo_externo ORDER BY
		  CASE
			WHEN UPPER(tipo) = 'PRINCIPAL' then 1
			WHEN UPPER(tipo) = 'PESSOAL' then 2
			WHEN UPPER(tipo) = 'ATENDIMENTO' then 3
			WHEN UPPER(tipo) = 'OUVIDORIA' then 4
			WHEN UPPER(tipo) = 'FINANCEIRO' then 5
			WHEN UPPER(tipo) = 'COMPRAS' then 6
			WHEN UPPER(tipo) = 'COMERCIAL' then 7
			ELSE 8
			END ) AS rn

		FROM {{ ref('dim_contrato_sydle') }} AS dim_contrato_sydle
		LEFT JOIN {{ ref('dim_cliente_emails') }} AS dim_cliente_emails
			ON dim_contrato_sydle.comprador_id_sydle = dim_cliente_emails.id_cliente
		WHERE NOT(
				UPPER(dim_cliente_emails.email) NOT LIKE '%@%'
				OR UPPER(dim_cliente_emails.email) LIKE '%ATUALIZ%'
				OR UPPER(dim_cliente_emails.email) LIKE '%SAC@SUMICITY%'
				OR UPPER(dim_cliente_emails.email) LIKE '%AUTALIZE%'
				or UPPER(dim_cliente_emails.email) LIKE '%TESTE%'
				OR UPPER(dim_cliente_emails.email) LIKE '%NAOTEM%'
				OR UPPER(dim_cliente_emails.email) LIKE '%N_OCONSTA%'
				OR UPPER(dim_cliente_emails.email) LIKE '%RCA@%'
				OR UPPER(dim_cliente_emails.email) LIKE '%SUMI%'
				OR UPPER(dim_cliente_emails.email) LIKE '%@TEM%')
	) A
	WHERE A.codigo_externo IS NOT NULL AND A.rn = 1
) AS email_sydle_1
	ON email_sydle_1.codigo_externo = dim_contrato.id_contrato_air

LEFT JOIN (
	SELECT A.*
	FROM (
		SELECT dim_contrato_sydle.codigo_externo,
		dim_cliente_emails.email,
		ROW_NUMBER() OVER (PARTITION BY dim_contrato_sydle.codigo_externo ORDER BY
		  CASE
			WHEN UPPER(tipo) = 'PRINCIPAL' then 1
			WHEN UPPER(tipo) = 'PESSOAL' then 2
			WHEN UPPER(tipo) = 'ATENDIMENTO' then 3
			WHEN UPPER(tipo) = 'OUVIDORIA' then 4
			WHEN UPPER(tipo) = 'FINANCEIRO' then 5
			WHEN UPPER(tipo) = 'COMPRAS' then 6
			WHEN UPPER(tipo) = 'COMERCIAL' then 7
			ELSE 8
			END ) AS rn

		FROM {{ ref('dim_contrato_sydle') }} AS dim_contrato_sydle
		LEFT JOIN {{ ref('dim_cliente_emails') }} AS dim_cliente_emails
			ON dim_contrato_sydle.comprador_id_sydle = dim_cliente_emails.id_cliente
		WHERE NOT(
				UPPER(dim_cliente_emails.email) NOT LIKE '%@%'
				OR UPPER(dim_cliente_emails.email) LIKE '%ATUALIZ%'
				OR UPPER(dim_cliente_emails.email) LIKE '%SAC@SUMICITY%'
				OR UPPER(dim_cliente_emails.email) LIKE '%AUTALIZE%'
				or UPPER(dim_cliente_emails.email) LIKE '%TESTE%'
				OR UPPER(dim_cliente_emails.email) LIKE '%NAOTEM%'
				OR UPPER(dim_cliente_emails.email) LIKE '%N_OCONSTA%'
				OR UPPER(dim_cliente_emails.email) LIKE '%RCA@%'
				OR UPPER(dim_cliente_emails.email) LIKE '%SUMI%'
				OR UPPER(dim_cliente_emails.email) LIKE '%@TEM%')
	) A
	WHERE A.codigo_externo IS NOT NULL AND A.rn = 2
) AS email_sydle_2
	ON email_sydle_2.codigo_externo = dim_contrato.id_contrato_air
)

SELECT DISTINCT A.id_cliente_air,
A.contrato_air,
A.status_contrato,
A.data_abertura_cadastro,
A.ativacao_contrato,
A.data_cancelamento_contrato,
A.dia_vencimento,
A.fechamento_vencimento,
A.login_contrato,
A.num_cpf_cnpj,
A.nome_cliente,
A.data_nascimento,
A.tipo_pessoa,
A.tipo_negocio,
A.canal_venda,
B.polo,
B.marca,
B.Regiao,
B.regional,
B.sub_regional,
B.cep,
B.estado,
B.cidade,
B.latitude,
B.longitude,
CA.email_air_1,
CA.email_air_2,
CA.tel_1_air,
CA.tel_2_air,
CA.tel_3_air,
CS.email_1_sydle,
CS.email_2_sydle,
CS.tel_1_sydle,
CS.tel_2_sydle,
CS.tel_3_sydle,
CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS dt_atualizacao

FROM cobranca_dados_cadastros A
left join cobranca_dados_endereco B
	ON A.contrato_air = B.contrato_air
left join cobranca_dados_contatos_air CA
	ON A.contrato_air = CA.contrato_air
left join cobranca_dados_contatos_sydle CS
	ON A.contrato_air = CS.contrato_air
