{% if not var("enable_crm_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_deals_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_hubspot_crm_deals') }}
  )
select * from t_deals_merge_list
