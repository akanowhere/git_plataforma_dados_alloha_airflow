WITH vw_sydle_tbl_usuario AS (
    SELECT *
    FROM {{ ref('vw_sydle_tbl_usuario') }}
)

SELECT
    id_usuario,
    data_criacao,
    data_alteracao,
    ativo,
    nome,
    login,
    email,
    data_integracao

FROM vw_sydle_tbl_usuario
