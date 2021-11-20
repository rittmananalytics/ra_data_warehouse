{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'google_ads' %}

with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__account_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__account_tmp')),
                staging_columns=get_google_ads_account_columns()
            )
        }}

    FROM base
),

final AS (

    SELECT
        id AS account_id,
        _fivetran_synced,
        account_label_name,
        currency_code,
        name AS account_name
    FROM fields
)

SELECT * FROM final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
