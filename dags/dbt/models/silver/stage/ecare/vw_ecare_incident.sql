{% set catalog_schema_table=source("ecare", "incident") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    incident_level_id,
    incident_type_id,
    status,
    responsible_team,
    {# protocol, #}
    active_ura,
    quantity_onu_affected,
    pon,
    slot,
    olt,
    description,
    problem_identifier,
    affected_tv,
    affected_internet,
    affected_phone,
    finalized_reason,
    incident_origin,
    {# integration_identifier, #}
    TRY_CAST(estimated_resolution_at AS TIMESTAMP) AS estimated_resolution_at,
    TRY_CAST(next_interaction_at AS TIMESTAMP) AS next_interaction_at,
    TRY_CAST(finalized_at AS TIMESTAMP) AS finalized_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    message

  FROM latest
)

SELECT *
FROM transformed
