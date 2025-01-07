WITH source AS (
  SELECT *
  FROM {{ source("atrixcore", "tblservicesvoip") }}
),

transformed AS (
  SELECT
  
batchid,
callsimultaneous,
ddd,
email,
id,
login,
password,
privacy	,
receivecalldue,
serviceid,
terminalid,
userid	

  FROM source
)

SELECT *
FROM transformed
