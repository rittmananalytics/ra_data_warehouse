{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('stg_facebook_ads__ad_history_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_facebook_ads__ad_history_tmp')),
                staging_columns=get_facebook_ad_history_columns()
            )
        }}

    from base
),

fields_xf as (

    select
        id as ad_id,
        account_id,
        ad_set_id,
        campaign_id,
        creative_id,
        name as ad_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields

)

select * from fields_xf

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
