{% if 'pinterest_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('pinterest_ads__ad_adapter')}}

), fields as (

    select
        cast(campaign_date as date) as date_day,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        cast(campaign_id as {{ dbt_utils.type_string() }}) as campaign_id,
        campaign_name,
        cast(ad_group_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        ad_group_name,
        platform,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(spend, 0) as spend
    from base


)

select *
from fields

{% else %} {{config(enabled=false)}} {% endif %}
