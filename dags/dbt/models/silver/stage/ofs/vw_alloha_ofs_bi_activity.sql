SELECT *
FROM {{ source('ofs_fieldintegration', 'alloha_ofs_bi_activity') }}
