{% set catalog_schema_table=source("sumicity_db_assine", "checkout") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    user_id,
    installation_addres_id,
    NULL AS billing_addres_id,  -- REMOVER OS NULL CASO AS COLUNAS TENHAM DADOS
    cart_id,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    legacy_status,
    viability_status_name,
    TRY_CAST(sold_at AS DATE) AS sold_at,
    sold_by,
    sale_notes,
    legacy_step,
    is_manual_approval_sale,
    digital_agent,
    failed_action_type,
    last_step,
    sale_restriction_type,
    sale_type,
    status,
    user_analysis_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    location_id,
    company_brand_id,
    coupon,
    coupon_discount_percentage,
    total_price,
    marketing_parameters,
    sale_id

  FROM latest
)

SELECT *
FROM transformed
