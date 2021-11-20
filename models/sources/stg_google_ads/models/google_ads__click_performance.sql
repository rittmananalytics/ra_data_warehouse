{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'adwords' %}

with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__click_performance') }}

), fields AS (

    SELECT
        date_day,
        campaign_id,
        ad_group_id,
        criteria_id,
        gclid,
        row_number() over (PARTITION BYgclid order by date_day) AS rn
    FROM base

), filtered AS ( -- we've heard that sometimes duplicates gclids are an issue. This dedupe ensures no glcids are double counted.

    SELECT *
    FROM fields
    where gclid is not null
    and rn = 1

)

SELECT * FROM filtered

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
