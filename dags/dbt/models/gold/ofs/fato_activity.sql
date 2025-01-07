{{
  config(
    materialized = 'incremental'
    )
}}

WITH stage_bi_activity AS (
  SELECT * FROM {{ ref('vw_alloha_ofs_bi_activity') }}
)

SELECT *
FROM
  stage_bi_activity

{% if is_incremental() %}

  WHERE updatedAt > (SELECT MAX(updatedAt) FROM {{ this }})

{% endif %}