{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with report AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__basic_ad') }}

), creatives AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history_prep') }}

), accounts AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__account_history') }}
    where is_most_recent_record = true

), ads AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__ad_history') }}
    where is_most_recent_record = true

), ad_sets AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__ad_set_history') }}
    where is_most_recent_record = true

), campaigns AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__campaign_history') }}
    where is_most_recent_record = true

), joined AS (

    SELECT
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        creatives.creative_id,
        creatives.creative_name,
        creatives.base_url,
        creatives.url_host,
        creatives.url_path,
        creatives.utm_source,
        creatives.utm_medium,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        sum(report.clicks) AS clicks,
        sum(report.impressions) AS impressions,
        sum(report.spend) AS spend
    FROM report
    left join ads
        on CAST(report.ad_id AS {{ dbt_utils.type_bigint() }}) = CAST(ads.ad_id AS {{ dbt_utils.type_bigint() }})
    left join creatives
        on CAST(ads.creative_id AS {{ dbt_utils.type_bigint() }}) = CAST(creatives.creative_id AS {{ dbt_utils.type_bigint() }})
    left join ad_sets
        on CAST(ads.ad_set_id AS {{ dbt_utils.type_bigint() }}) = CAST(ad_sets.ad_set_id AS {{ dbt_utils.type_bigint() }})
    left join campaigns
        on CAST(ads.campaign_id AS {{ dbt_utils.type_bigint() }}) = CAST(campaigns.campaign_id AS {{ dbt_utils.type_bigint() }})
    left join accounts
        on CAST(report.account_id AS {{ dbt_utils.type_bigint() }}) = CAST(accounts.account_id AS {{ dbt_utils.type_bigint() }})
    {{ dbt_utils.group_by(19) }}


)

SELECT *
FROM joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
