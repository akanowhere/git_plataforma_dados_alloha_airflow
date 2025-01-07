WITH vw_assine_tbl_checkout AS (
  SELECT *
  FROM {{ ref('vw_assine_tbl_checkout') }}
),

vw_assine_tbl_user_contract AS (
  SELECT *
  FROM {{ ref('vw_assine_tbl_user_contract') }}
),

joined AS (
  SELECT
    ck.coupon AS cupom,
    uc.contract_air_id AS contrato_air

  FROM vw_assine_tbl_checkout AS ck
  LEFT JOIN vw_assine_tbl_user_contract AS uc ON ck.id = uc.checkout_id AND ck.user_id = uc.user_id

  WHERE 1 = 1
    AND ck.coupon IS NOT NULL
    AND ck.status IN ('FINISHED', 'MANUALLY_APPROVED')
)

SELECT *
FROM joined
