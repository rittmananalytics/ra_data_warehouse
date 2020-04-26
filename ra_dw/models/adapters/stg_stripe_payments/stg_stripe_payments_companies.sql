{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_stripe','s_charges') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed as (
select * from (
SELECT
concat('stripe-',charge_client_name) AS company_id,
    replace(replace(replace(charge_client_name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
    cast (null as string) as company_address,
    cast (null as string) AS company_address2,
    cast (null as string) AS company_city,
    cast (null as string) AS company_state,
    cast (null as string) AS company_country,
    cast (null as string) AS company_zip,
    cast (null as string) AS company_phone,
    cast (null as string) AS company_website,
    cast (null as string) AS company_industry,
    cast (null as string) AS company_linkedin_company_page,
    cast (null as string) AS company_linkedin_bio,
    cast (null as string) AS company_twitterhandle,
    cast (null as string) AS company_description,
    cast (null as string) as company_finance_status,
    min(charge_created_ts) over (partition by charge_client_name) as company_created_date,
    max(charge_created_ts) over (partition by charge_client_name) as company_last_modified_date
    FROM `ra-development.mark_bi_apps_dev_staging.stg_stripe_payments_charges` )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
select * from renamed
