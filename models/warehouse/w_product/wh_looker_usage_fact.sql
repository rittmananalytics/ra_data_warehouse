{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='looker_usage_fact'
    )
}}
{% endif %}

with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
),
contacts_dim as (
    select *
    from {{ ref('wh_contacts_dim') }}
)
SELECT
   GENERATE_UUID() as looker_usage_pk,
   c.company_pk,
   u.contact_pk,
   d.* except (company_id,contact_id,pk)
FROM
   {{ ref('int_looker_usage') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)
JOIN contacts_dim u
      ON d.contact_id IN UNNEST(u.all_contact_ids)
