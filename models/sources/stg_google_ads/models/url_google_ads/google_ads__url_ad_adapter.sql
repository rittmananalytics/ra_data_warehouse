{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'google_ads' %}

with stats AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_stats') }}

), accounts AS (

    SELECT *
    FROM {{ ref('stg_google_ads__account') }}

), campaigns AS (

    SELECT *
    FROM {{ ref('stg_google_ads__campaign_history') }}
    where is_most_recent_record = True

), ad_groups AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_group_history') }}
    where is_most_recent_record = True

), ads AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_history') }}
    where is_most_recent_record = True

), final_url AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_final_url_history') }}
    where is_most_recent_record = True

), fields AS (

    SELECT
        stats.date_day,
        accounts.account_name,
        accounts.account_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        ad_groups.ad_group_name,
        ad_groups.ad_group_id,
        final_url.base_url,
        final_url.url_host,
        final_url.url_path,
        final_url.utm_source,
        final_url.utm_medium,
        final_url.utm_campaign,
        final_url.utm_content,
        final_url.utm_term,
        sum(stats.spend) AS spend,
        sum(stats.clicks) AS clicks,
        sum(stats.impressions) AS impressions

        {% for metric in var('google_ads__ad_stats_passthrough_metrics') %}
        , sum(stats.{{ metric }}) AS {{ metric }}
        {% endfor %}

    FROM stats
    left join ads
        on stats.ad_id = ads.ad_id
    left join final_url
        on ads.ad_id = final_url.ad_id
    left join ad_groups
        on ads.ad_group_id = ad_groups.ad_group_id
    left join campaigns
        on ad_groups.campaign_id = campaigns.campaign_id
    left join accounts
        on campaigns.account_id = accounts.account_id
    {{ dbt_utils.group_by(15) }}

)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
