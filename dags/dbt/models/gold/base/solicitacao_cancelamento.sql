with contrato_entrega AS (
    SELECT a.id_contrato, max(a.id_endereco) AS endereco
    FROM {{ ref("vw_air_tbl_contrato_entrega") }} a
    WHERE a.excluido = false
    GROUP BY a.id_contrato
)

select
       t1.id,
       t1.data_criacao,
       t1.usuario_criacao,
       t7.email as email_usuario_criacao,
       t1.data_alteracao,
       t1.usuario_alteracao,
       t8.email as email_usuario_alteracao,
       t1.id_contrato as contrato_air,
       t1.primeiro_nivel as cod_primeiro_nivel,
       t2.nome as primeiro_nivel,
       t1.segundo_nivel as cod_segundo_nivel,
       t3.nome as segundo_nivel,
       t1.motivo as cod_motivo,
       t4.nome as motivo,
       t1.submotivo as cod_submotivo,
       t5.nome as submotivo,
       t1.observacao,
       t1.servico,
       t1.tipo_cliente as cod_tipo_cliente,
       t6.nome as tipo_cliente,
       t11.nome as cidade,
       t11.estado,
       CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
from {{ ref("vw_air_tbl_perfilacao_retencao") }} t1
left join {{ ref("vw_air_tbl_catalogo_item") }} t2 on (t1.primeiro_nivel = t2.codigo and t2.excluido = 0)
left join {{ ref("vw_air_tbl_catalogo_item") }} t3 on (t1.segundo_nivel = t3.codigo and t3.excluido = 0)
left join {{ ref("vw_air_tbl_catalogo_item") }} t4 on (t1.motivo = t4.codigo and t4.excluido = 0)
left join {{ ref("vw_air_tbl_catalogo_item") }} t5 on (t1.submotivo = t5.codigo and t5.excluido = 0)
left join {{ ref("vw_air_tbl_catalogo_item") }} t6 on (t1.tipo_cliente = t6.codigo and t6.excluido = 0)
left join {{ ref("vw_air_tbl_usuario") }} t7 on (t1.usuario_criacao = t7.codigo )
left join {{ ref("vw_air_tbl_usuario") }} t8 on (t1.usuario_criacao = t8.codigo )
LEFT JOIN contrato_entrega AS t9 on (t1.id_contrato = t9.id_contrato)
left join {{ ref("vw_air_tbl_endereco") }} t10 on (t9.endereco = t10.id)
left join {{ ref("vw_air_tbl_endereco_cidade") }} t11 on (t10.id_cidade= t11.id)
where date(t1.data_criacao) >= '2023-09-01' and t1.excluido = false
