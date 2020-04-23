{% if not var("enable_crm_warehouse") or not var("enable_hubspot_crm_source")%}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_communications_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_hubspot_crm_communications') }}
  )
select * from t_communications_merge_list
