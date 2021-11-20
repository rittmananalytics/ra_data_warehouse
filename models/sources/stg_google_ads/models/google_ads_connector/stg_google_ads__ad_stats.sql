{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'google_ads' %}

with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_stats_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__ad_stats_tmp')),
                staging_columns=get_google_ads_ad_stats_columns()
            )
        }}

    FROM base
),

final AS (

    SELECT
        customer_id AS account_id,
        date AS date_day,
        ad_group AS ad_group_id,
        ad_id,
        campaign_id,
        clicks,
        cost_micros / 1000000.0 AS spend,
        impressions

        {% for metric in [] %}
        , {{ metric }}
        {% endfor %}
    FROM fields
)

SELECT * FROM final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
