{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__account_history_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_facebook_ads__account_history_tmp')),
                staging_columns=get_facebook_account_history_columns()
            )
        }}

    FROM base
),

fields_xf AS (

    SELECT
        id AS account_id,
        name AS account_name,
        row_number() over (PARTITION BYid order by _fivetran_synced desc) = 1 AS is_most_recent_record
    FROM fields

)

SELECT * FROM fields_xf

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
