{{ config(enabled=var('ad_reporting__snapchat_ads_enabled')) }}

with base as (

    select *
    from {{ ref('snapchat__ad_adapter')}}

), fields as (

    select
        cast(date_day as date) as date_day,
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
        cast(ad_squad_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        ad_squad_name as ad_group_name,
        'Snapchat Ads' as platform,
        sum(coalesce(swipes, 0)) as swipes,
        sum(coalesce(impressions, 0)) as impressions,
        sum(coalesce(spend, 0)) as spend
    from base
    {{ dbt_utils.group_by(14) }}


)

select *
from fields