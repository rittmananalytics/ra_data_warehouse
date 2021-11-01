{{ config(enabled=var('ad_reporting__facebook_ads_enabled')) }}

with base as (

    select *
    from {{ ref('facebook_ads__ad_adapter')}}

), fields as (

    select
        cast(date_day as date) as date_day,
        account_name,
        cast(account_id as {{ dbt_utils.type_string() }}) as account_id,
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
        cast(ad_set_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        ad_set_name as ad_group_name,
        'Facebook Ads' as platform,
        sum(coalesce(clicks, 0)) as clicks,
        sum(coalesce(impressions, 0)) as impressions,
        sum(coalesce(spend, 0)) as spend
    from base
    {{ dbt_utils.group_by(16) }}


)

select *
from fields