{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'adwords' %}

with base as (

    select *
    from {{ ref('stg_google_ads__click_performance') }}

), fields as (

    select
        date_day,
        campaign_id,
        ad_group_id,
        criteria_id,
        gclid,
        row_number() over (partition by gclid order by date_day) as rn
    from base

), filtered as ( -- we've heard that sometimes duplicates gclids are an issue. This dedupe ensures no glcids are double counted.

    select *
    from fields
    where gclid is not null
    and rn = 1

)

select * from filtered

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
