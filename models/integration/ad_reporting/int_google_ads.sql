{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('google_ads__url_ad_adapter')}}

), fields as (

    select
        'Google Ads' as platform,
        cast(date_day as date) as date_day,
        account_name,
        {% if var('google_ads_api_source','adwords') == 'google_ads' %} account_id {% else %} external_customer_id as account_id {% endif %} ,
        campaign_name,
        cast(campaign_id as {{ dbt_utils.type_string() }}) as campaign_id,
        ad_group_name,
        cast(ad_group_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(spend, 0) as spend
    from base

)

select *
from fields

{% else %} {{config(enabled=false)}} {% endif %}
