{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__ad_adapter')}}

), fields AS (

    SELECT
        CAST(date_day AS date) AS date_day,
        account_name,
        CAST(account_id AS {{ dbt_utils.type_string() }}) AS account_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        CAST(campaign_id AS {{ dbt_utils.type_string() }}) AS campaign_id,
        campaign_name,
        CAST(ad_set_id AS {{ dbt_utils.type_string() }}) AS ad_group_id,
        ad_set_name AS ad_group_name,
        'Facebook Ads' AS platform,
        sum(coalesce(clicks, 0)) AS clicks,
        sum(coalesce(impressions, 0)) AS impressions,
        sum(coalesce(spend, 0)) AS spend
    FROM base
    {{ dbt_utils.group_by(16) }}


)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
