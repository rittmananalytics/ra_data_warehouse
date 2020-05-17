{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if not var("enable_finance_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='invoice_pk',
        alias='invoices_fact'
    )
}}
{% endif %}

WITH invoices AS
  (
  SELECT *
  FROM   {{ ref('int_invoices') }}
  ),
  companies_dim as (
      select *
      from {{ ref('wh_companies_dim') }}
  )
{% if var("enable_harvest_projects_source") %},
  projects_dim as (
      select *
      from {{ ref('wh_timesheet_projects_dim') }}
),
  user_dim as (
    select *
    from {{ ref('wh_users_dim') }}
)
{% endif %}
SELECT
   GENERATE_UUID() as invoice_pk,
   c.company_pk,
   row_number() over (partition by c.company_pk order by invoice_sent_at_ts) as invoice_seq,
   date_diff(date(invoice_sent_at_ts),min(date(invoice_sent_at_ts)) over (partition by c.company_pk),MONTH) as months_since_first_invoice,
   timestamp(date_trunc(min(date(invoice_sent_at_ts)) over (partition by c.company_pk),MONTH)) first_invoice_month,
   date_diff(date(invoice_sent_at_ts),min(date(invoice_sent_at_ts)) over (partition by c.company_pk),QUARTER) as quarters_since_first_invoice,
   timestamp(date_trunc(min(date(invoice_sent_at_ts)) over (partition by c.company_pk),QUARTER)) first_invoice_quarter,
{% if var("enable_harvest_projects_source") %}
   s.user_pk as creator_users_pk,
   p.timesheet_project_pk,
{% endif %}
   i.*
FROM
   invoices i
JOIN companies_dim c
      ON i.company_id IN UNNEST(c.all_company_ids)
{% if var("enable_harvest_projects_source") %}
JOIN user_dim s
   ON cast(i.invoice_creator_users_id as string) IN UNNEST(s.all_user_ids)
JOIN projects_dim p
   ON cast(i.project_id as string) = p.timesheet_project_id
{% endif %}

       cast(canonical_vid as string) as contact_id,
       properties.firstname.value as contact_first_name,
       properties.lastname.value as contact_last_name,
       coalesce(concat(properties.firstname.value,' ',properties.lastname.value),properties.email.value) as contact_name,
       properties.jobtitle.value contact_job_title,
       properties.email.value as contact_email,
       properties.phone.value as contact_phone,
       properties.mobilephone.value as contact_mobile_phone,
       properties.address.value contact_address,
       properties.city.value contact_city,
       properties.state.value contact_state,
       properties.country.value as contact_country,
       properties.zip.value contact_postcode_zip,
       properties.company.value contact_company,
       properties.website.value contact_website,
       cast(properties.associatedcompanyid.value as string) as contact_company_id,
       cast(properties.hubspot_owner_id.value as string) as contact_owner_id,
       properties.lifecyclestage.value as contact_lifecycle_stage,
       properties.createdate.value as contact_created_date,
       properties.lastmodifieddate.value as contact_last_modified_date,
    from source
)
{% endif %}
select * from renamed
