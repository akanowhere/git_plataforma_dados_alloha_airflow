{% set catalog_schema_table=source("sumicity_db_assine", "user_analysis") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    transaction_identifier,
    user_contract_id,
    user_id,
    provided_user_name,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    legacy_lead_type,
    lead_message,
    total_elapsed_time,
    sa_request_status,
    sa_address_formatted,
    sa_availability_status,
    sa_location_type,
    sa_address_sent,
    sa_availability_radius,
    sa_address_distance,
    sa_address_walk_distance,
    sa_config_enabled,
    sa_walk_config_enabled,
    sa_elapsed_time,
    availability_approved,
    spc_request_status,
    spc_approved,
    spc_document_status,
    spc_name_status,
    spc_mother_name_status,
    spc_birthdate_status,
    spc_message,
    spc_number_of_restrictions,
    spc_total_restrictions_value,
    spc_config_enabled,
    spc_elapsed_time,
    financial_analysis_approved,
    serasa_note,
    serasa_decision,
    serasa_status,
    cs_request_status,
    NULL AS cs_id,  -- REMOVER OS NULL CASO AS COLUNAS TENHAM DADOS
    TRY_CAST(NULL AS DATE) AS cs_reference_date,
    NULL AS cs_identifier,
    cs_value,
    NULL AS cs_reason,
    cs_approved,
    cs_should_validate_sms,
    NULL AS cs_sms_validated,
    cs_config_maximum_score,
    cs_config_enabled,
    cs_elapsed_time,
    city_use_assine_on_register,
    zip_code_config_enable,
    legacy_latest_step,
    partner_id,
    lomadee_partner_id,
    sa_state_location_name,
    TRY_CAST(sold_at AS DATE) AS sold_at,
    sold_by,
    sale_notes,
    sa_point_id,
    failed_action_type,
    last_checkout_status,
    last_checkout_step,
    sale_restriction_type,
    sale_type,
    lead_air_id,
    personal_approved

  FROM latest
)

SELECT *
FROM transformed
