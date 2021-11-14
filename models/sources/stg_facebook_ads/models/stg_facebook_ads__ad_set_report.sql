{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with adapter as (

    select *
    from {{ ref('stg_facebook_ads__ad_adapter') }}

), aggregated as (

    select
        date_day,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_set_id,
        ad_set_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend
    from adapter
    {{ dbt_utils.group_by(7) }}

)

select *
from aggregated

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
