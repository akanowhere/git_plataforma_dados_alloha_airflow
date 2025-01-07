{% set catalog_schema_table=source('infobip', 'disparos_whatsapp') %}
{% set partition_column="Message_Id" %}
{% set order_column="_file_generation_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    Account_Name AS account_name,
    Traffic_Source AS traffic_source,
    Communication_Name AS communication_name,
    Communication_Scheduled_For AS communication_scheduled_for,
    Communication_Start_Date AS communication_start_date,
    Communication_Template AS communication_template,
    From AS from,
    To AS to,
    Message_Id AS message_id,
    TO_TIMESTAMP(Send_At, 'dd/MM/yyyy HH:mm:ss') AS send_at,
    TRY_CAST(Country_Prefix AS INT) AS country_prefix,
    Country_Name AS country_name,
    Network_Name AS network_name,
    TRY_CAST(Purchase_Price AS FLOAT) AS purchase_price,
    Status AS status,
    Reason AS reason,
    Action AS action,
    Error_Group AS error_group,
    Error_Name AS error_name,
    TO_TIMESTAMP(COALESCE(NULLIF(TRIM(Done_At), ''), NULL), 'dd/MM/yyyy HH:mm:ss') AS done_at,
    Text AS text,
    TRY_CAST(Messages_Count AS INT) AS messages_count,
    Service_Name AS service_name,
    User_Name AS user_name,
    TO_TIMESTAMP(COALESCE(NULLIF(TRIM(Seen_At), ''), NULL), 'dd/MM/yyyy HH:mm:ss') AS seen_at,
    Clicks AS clicks,
    Paired_Message_Id AS paired_message_id,
    Data_Payload AS data_payload,
    Campaign_Reference AS campaign_reference,
    Application_ID AS application_id,
    Entity_ID AS entity_id,
    CAST(_file_generation_date AS DATE) AS _file_generation_date

  FROM latest
)

SELECT *
FROM
  transformed
