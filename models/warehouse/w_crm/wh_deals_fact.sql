{% if not var("enable_crm_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='deals_fact',
        unique_key='deal_id',
        incremental_strategy='merge',
        materialized='incremental'
    )
}}
{% endif %}

with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
)
SELECT
   GENERATE_UUID() as deal_pk,
   c.company_pk,
   d.* except (company_id)
FROM
   {{ ref('int_deals') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)

{% if is_incremental() %}
     -- this filter will only be applied on an incremental run
     where deal_created_date > (select max(deal_created_date) from {{ this }})
     or    deal_last_modified_date > (select max(deal_last_modified_date) from {{ this }})
{% endif %}
