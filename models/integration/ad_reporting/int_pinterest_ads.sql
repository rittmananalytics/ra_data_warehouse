{% if 'pinterest_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('pinterest_ads__ad_adapter')}}

), fields AS (

    SELECT
        CAST(campaign_date AS date) AS date_day,
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
        CAST(ad_group_id AS {{ dbt_utils.type_string() }}) AS ad_group_id,
        ad_group_name,
        platform,
        coalesce(clicks, 0) AS clicks,
        coalesce(impressions, 0) AS impressions,
        coalesce(spend, 0) AS spend
    FROM base


)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
