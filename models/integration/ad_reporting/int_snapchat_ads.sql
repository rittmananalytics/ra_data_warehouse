{% if 'snapchat_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('snapchat__ad_adapter')}}

), fields AS (

    SELECT
        CAST(date_day AS date) AS date_day,
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
        CAST(ad_squad_id AS {{ dbt_utils.type_string() }}) AS ad_group_id,
        ad_squad_name AS ad_group_name,
        'Snapchat Ads' AS platform,
        sum(coalesce(swipes, 0)) AS swipes,
        sum(coalesce(impressions, 0)) AS impressions,
        sum(coalesce(spend, 0)) AS spend
    FROM base
    {{ dbt_utils.group_by(14) }}


)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
