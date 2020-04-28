{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  select * from
  (
    select *,
           max(_fivetran_synced) OVER (PARTITION BY pk ORDER BY _fivetran_synced RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_fivetran_synced
    from
    {{ source(
      'looker_usage',
      's_usage_stats'
    ) }}
  )
  where _fivetran_synced = max_fivetran_synced
),
renamed as (
select * from (
SELECT
concat('looker-',client) AS company_id,
    client AS company_name,
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
    min(timestamp(created_time)) over (partition by client) as company_created_date,
    max(timestamp(created_time)) over (partition by client) as company_last_modified_date
    FROM source )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
select * from renamed
