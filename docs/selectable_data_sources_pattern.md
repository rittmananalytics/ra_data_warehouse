## Selectable Data Sources

The particular data sources enabled for a warehouse implementation can be enabled or disabled by the setting of a flag in the dbt_project.yml configuration file:

```yaml
vars:
      enable_harvest_projects_source:      [true|false]
      enable_hubspot_crm_source:           [true|false]
      enable_asana_projects_source:        [true|false]
      enable_jira_projects_source:         [true|false]
      enable_stripe_payments_source:       [true|false]
      enable_xero_accounting_source:       [true|false]
      enable_mailchimp_email_source:       [true|false]
      enable_segment_events_source:        [true|false]
      enable_google_ads_source:            [true|false]
      enable_facebook_ads_source:          [true|false]
      enable_intercom_messaging_source:    [true|false]
      enable_custom_source_1:              [true|false]
      enable_custom_source_2:              [true|false]
      enable_mixpanel_events_source:       [true|false]
# warehouse modules
      enable_crm_warehouse:         [true|false]e
      enable_finance_warehouse:     [true|false]
      enable_projects_warehouse:    [true|false]
      enable_marketing_warehouse:   [true|false]
      enable_ads_warehouse:         [true|false]
      enable_product_warehouse:     [true|false]
```

Within each data source, if supported you can also select the etl technology:

```yaml
stg_mixpanel_events:
              vars:
                  id-prefix: [Your datasource prefix, unique for each source, e.g. mixpanel-]
                  etl: [fivetran|stitch]
                  fivetran_event_table: [Your Fivetran dataset and table name,fivetran_mixpanel.event]
                  stitch_export_table: [Your Stitch dataset and table name, e.g. mixpanel_stitch.export]
```

## Design Pattern

1. Source transformation views reference these flags in the dbt jinja template code:

```
{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('ads_insights_table'),'ad_id') }}
),
```

2. int_ merge and deduplication views also reference these flags in their jinja template code:

```
{% if not var("enable_crm_warehouse") and not var("enable_finance_warehouse") and not var("enable_marketing_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_contacts_merge_list as
  (
    {% if var("enable_hubspot_crm_source") %}
    SELECT * except (contact_id, contact_company_id),
           concat('hubspot-',contact_id) as contact_id,
           concat('hubspot-',contact_company_id) as contact_company_id
    FROM   {{ ref('stg_hubspot_crm_contacts') }}
    {% endif %}
    {% if var("enable_hubspot_crm_source") and var("enable_harvest_projects_source") is true %}
    UNION ALL
    {% endif %}
    {% if var("enable_harvest_projects_source") is true %}
    SELECT * except (contact_id, contact_company_id),
           concat('harvest-',contact_id) as contact_id,
           concat('harvest-',contact_company_id) as contact_company_id
    FROM   {{ ref('stg_harvest_projects_contacts') }}
    {% endif %}
```
3. wh_ dimension and fact tables reference these flags for both sources and warehouse modules:

```
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

{% if var("enable_harvest_projects_source") %}
,
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
```

4. Where a data source supports multiple ETL pipeline technologies, an `etl` variable scoped to the particular data source is used within the model to execute the correct pipeline-specific SQL and jinja code.

```
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

```
