{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__basic_ad_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_facebook_ads__basic_ad_tmp')),
                staging_columns=get_facebook_basic_ad_columns()
            )
        }}

    FROM base
),

final AS (

    SELECT
        ad_id,
        date AS date_day,
        account_id,
        impressions,
        inline_link_clicks AS clicks,
        spend
    FROM fields
)

SELECT * FROM final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
