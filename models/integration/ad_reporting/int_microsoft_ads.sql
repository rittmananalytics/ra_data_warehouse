{% if 'microsoft_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('microsoft_ads__ad_adapter')}}

), fields AS (

    SELECT
        'Microsoft Ads' AS platform,
        CAST(date_day AS date) AS date_day,
        account_name,
        CAST(account_id AS {{ dbt_utils.type_string() }}) AS account_id,
        campaign_name,
        CAST(campaign_id AS {{ dbt_utils.type_string() }}) AS campaign_id,
        ad_group_name,
        CAST(ad_group_id AS {{ dbt_utils.type_string() }}) AS ad_group_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        coalesce(clicks, 0) AS clicks,
        coalesce(impressions, 0) AS impressions,
        coalesce(spend, 0) AS spend
    FROM base


)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
