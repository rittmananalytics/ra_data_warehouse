# What's New in Version 1.2.0

1. Reworking all of the stg_ source modules to use dbt sources and source freshness metadata, as has become the standard after recent client project implementations of the framework

2. Adopted the new way of selecting which sources to implement for companies, contacts, campaigns and other sources we merge together as well as the new way of defining variables in dbt version 17.0+;  to specify which sources of company data we merge together there's a variable in the dbt_project.yml file like this:

```
vars:
  crm_warehouse_company_sources: ['hubspot_crm','harvest_projects','xero_accounting','stripe_payments','asana_projects','jira_projects','looker_usage']
```

Then the int_companies_pre_merged.sql model first checks there's are at least one source of company data to merge together:

```
{% if var('crm_warehouse_company_sources') %}
```

and then merges the ones listed in the variable array using a for loop:

```
{% for source in var('crm_warehouse_company_sources') %}
      {% set relation_source = 'stg_' + source + '_companies' %}
      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}
        {% if not loop.last %}union all{% endif %}
      {% endfor %}
    ),
```

3. Adopted the use of dbt_utils.surrogate_key to replace the BigQuery-specific GENERATE_UUID()

```
select
      {{ dbt_utils.surrogate_key(
      ['contact_pk','deal_pk']
      ) }} as contact_deal_pk,
```

4. And throughout the project, made use of dbt_utils cross-database functions as much as possible, for example

```
SELECT {{ dbt_utils.star(from=ref('int_delivery_tasks')) }}
  FROM   {{ ref('int_delivery_tasks') }}
and
case when fields.status.name	 not in ('Done','Done/Passed Client QA') then timestamp_diff({{ dbt_utils.current_timestamp() }},fields.created,HOUR)
         end as total_task_hours_incomplete,
```

5. Items #3 and #4 were in support of the last new feature - the framework now also runs on Snowflake Data Warehouse, using this same dbt package and git repo. Depending on whether you run the package with BigQuery or Snowflake as the target, e.g.:

```
dbt run --profile ra_data_warehouse --target prod
or
dbt run --profile ra_data_warehouse --target snowflake_dev
```

as long as you've got the two destinations defined properly in your profiles.yml file:

```
ra_data_warehouse:
  outputs:
    snowflake_dev:
      type: snowflake
      account: **********
      user: **********
      password: **********
      role: ******
      database: **********
      warehouse: **********
      schema: ANALYTICS
      threads: 1
      client_session_keep_alive: False
      query_tag: dbt
    prod:
      type: bigquery
      method: service-account-json
      project: **********
      dataset: analytics
      location: europe-west2
      threads: 1
      timeout_seconds: 300
      keyfile_json:
        type: service_account
        project_id: **********
        private_key_id: **********
        private_key: **********
        client_email: **********@**********.iam.gserviceaccount.com
        client_id: **********
        auth_uri: https://accounts.google.com/o/oauth2/auth
        token_uri: https://oauth2.googleapis.com/token
        auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
        client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/dbt-578%40ra-development.iam.gserviceaccount.com
```

then the jinja code in the project will enable either the BigQuery or Snowflake versions of the various stg_ modules, like this:

```
{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_jira_projects_stitch_issues_table'),unique_column='key') }}
),
```

and the int_ and wh_ modules should as much as possible be target system agnostic; where this isn't possible then there's conditional execution code in the models that detects what platform it's running on and runs the SQL appropriate for that platform:

```
  {% if target.type == 'bigquery' %}
    JOIN companies_dim C
    ON t.company_id IN unnest(
      C.all_company_ids
    )
    JOIN contacts_dim u
    ON CAST(
      t.timesheet_users_id AS STRING
    ) IN unnest(
      u.all_contact_ids
    )
  {% elif target.type == 'snowflake' %}
    JOIN companies_dim C
    ON t.company_id = C.company_id
    JOIN contacts_dim u
    ON t.timesheet_users_id :: STRING = u.contact_id
{% else %}
  {{ exceptions.raise_compiler_error(
    target.type ~ " not supported in this project"
  ) }}
```
