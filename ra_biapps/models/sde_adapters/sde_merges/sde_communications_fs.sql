{% if not var("enable_crm_warehouse") or not var("enable_hubspot_crm_source")%}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sde_communications_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_hubspot_crm_communications') }}
  )
select * from sde_communications_merge_list
