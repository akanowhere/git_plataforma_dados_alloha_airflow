{{
  config(
    materialized = 'incremental'
    )
}}

WITH stage_bi_inventory AS (
  SELECT * FROM {{ ref('vw_alloha_ofs_bi_inventory') }}
)

SELECT *
FROM
  stage_bi_inventory

{% if is_incremental() %}

  WHERE updatedAt > (SELECT MAX(updatedAt) FROM {{ this }})

{% endif %}
