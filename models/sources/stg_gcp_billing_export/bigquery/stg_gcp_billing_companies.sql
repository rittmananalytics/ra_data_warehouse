{{config(enabled = target.type == 'bigquery')}}
{% if not var("enable_gcp_billing_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
    SELECT
      *
    FROM
      {{ source('gcp_billing', 'gcp_billing_export') }}
  ),
 renamed as
 (
  SELECT
  select
         concat('gcp-',project.id  as string)) as company_id,
         name as company_name,
         cast (null as string) as company_address,
         cast (null as string) AS company_address2,
         cast (null as string) as company_city,
         cast (null as string) as company_state,
         cast (null as string) as company_country,
         cast (null as string) as company_zip,
         cast (null as string) as company_phone,
         cast (null as string) AS company_website,
         cast (null as string) AS company_industry,
         cast (null as string) AS company_linkedin_company_page,
         cast (null as string) AS company_linkedin_bio,
         cast (null as string) AS company_twitterhandle,
         cast (null as string) AS company_description,
         cast (null as string) as company_finance_status,
         cast(null as timestamp) as company_created_date,
         cast(null as timestamp) as company_last_modified_date
  FROM source
  GROUP BY 1)
SELECT
 *
FROM
 renamed
