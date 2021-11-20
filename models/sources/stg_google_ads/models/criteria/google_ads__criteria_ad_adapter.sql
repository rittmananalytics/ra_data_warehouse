{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'adwords' %}

with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__criteria_performance') }}

), fields AS (

    SELECT
        date_day,
        account_name,
        external_customer_id,
        campaign_name,
        campaign_id,
        ad_group_name,
        ad_group_id,
        criteria,
        criteria_type,
        sum(spend) AS spend,
        sum(clicks) AS clicks,
        sum(impressions) AS impressions

        {% for metric in var('google_ads__criteria_passthrough_metrics') %}
        , sum({{ metric }}) AS {{ metric }}
        {% endfor %}
    FROM base
    {{ dbt_utils.group_by(9) }}

)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
