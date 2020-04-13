{% if not var("enable_crm_warehouse") or not var("enable_hubspot_crm_source")%}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sde_deals_fs_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_hubspot_crm_deals') }}
  )
select * from sde_deals_fs_merge_list
