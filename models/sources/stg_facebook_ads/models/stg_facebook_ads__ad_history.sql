{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__ad_history_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_facebook_ads__ad_history_tmp')),
                staging_columns=get_facebook_ad_history_columns()
            )
        }}

    FROM base
),

fields_xf AS (

    SELECT
        id AS ad_id,
        account_id,
        ad_set_id,
        campaign_id,
        creative_id,
        name AS ad_name,
        row_number() over (PARTITION BYid order by _fivetran_synced desc) = 1 AS is_most_recent_record
    FROM fields

)

SELECT * FROM fields_xf

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
