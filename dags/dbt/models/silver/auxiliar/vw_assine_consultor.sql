WITH vw_assine_tbl_user_analysis AS (
  SELECT *
  FROM {{ ref('vw_assine_tbl_user_analysis') }}
),

vw_assine_tbl_user_contract AS (
  SELECT *
  FROM {{ ref('vw_assine_tbl_user_contract') }}
),

joined AS (
  SELECT
    ua.user_contract_id,
    ua.partner_id,
    ua.legacy_latest_step,
    ua.legacy_lead_type,
    uc.id,
    uc.contract_air_id,
    uc.hiring_date

  FROM vw_assine_tbl_user_analysis AS ua
  LEFT JOIN vw_assine_tbl_user_contract AS uc ON ua.user_contract_id = uc.id
),

unioned AS (
  SELECT DISTINCT
    contract_air_id,
    hiring_date

  FROM joined

  WHERE 1 = 1
    AND partner_id = 'lomadee'
    AND legacy_latest_step = 'FINISH'
    AND legacy_lead_type = 'OK'
    AND hiring_date > '2023-02-28'

  UNION

  SELECT DISTINCT
    contract_air_id,
    hiring_date

  FROM joined

  WHERE 1 = 1
    AND partner_id = 'socialsoul'
    AND legacy_latest_step = 'FINISH'
    AND legacy_lead_type = 'OK'
    AND hiring_date > '2023-02-28'
)

SELECT *
FROM unioned
