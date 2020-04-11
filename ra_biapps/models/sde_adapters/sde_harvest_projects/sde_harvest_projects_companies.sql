WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('harvest_projects','clients') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed as (

  SELECT
  'harvest_projects' as source,
  id AS company_id,
  replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
  address as company_address,
  created_at as company_created_date,
  updated_at as company_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed
