{% if not var("enable_crm_warehouse") and not enable_finance_warehouse and not enable_marketing_warehouse and not enable_projects_warehouse %}

{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='contact_pk',
        alias='contacts_dim'
    )
}}
{% endif %}

WITH contacts AS
  (
  SELECT *
  FROM
     {{ ref('int_contacts') }} c
)
select    {{ dbt_utils.surrogate_key(
          ['contact_name']
          ) }} as contact_pk,
          *
          FROM
          contacts c
