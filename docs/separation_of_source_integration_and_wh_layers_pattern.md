## Separation of Data Sources, Integration and Warehouse Module Layers

There are three distinct layers in the data warehouse:

1. A layer of source and ETL pipeline-specific data sources, containing SQL code used to transform and rename incoming tables from each source into common formats

2. An Integration layer, containing SQL transformations used to integrate, merge, deduplicate and transform data ready for loading into the main warehouse fact and dimension tables.

3. A warehouse layer made-up of subject area data marts, each of which contains multiple fact and conformed dimension tables

![Model Layers](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/data_flow.png)

## Design Pattern

dbt models inside this project are grouped together by these layers, with each data source "adapter" having all of its source SQL transformations contained with it.

```
├── analysis
├── data                      <-- "seed" files used for matching/merging companies, projects etc
├── macros
├── models
│   ├── integration           <-- "integration" models used to merge and dedupe models across multiple sources
│   ├── sources
│   │   ├── stg_asana_projects.        <-- "source" models with data-source specific transformations and
│   │   ├── stg_custom_source_1            renaming of columns into common formats. Where more than one
│   │   ├── stg_custom_source_2            pipeline technology (Stitch, Fivetran etc) is supported, these will
│   │   ├── stg_facebook_ads               contain SQL and jinja code for each pipeline type within the one model
│   │   ├── stg_gcp_billing_export         with the etl type configurable in the dbt_project.yml config file
│   │   ├── stg_google_ads
│   │   ├── stg_harvest_projects
│   │   ├── stg_hubspot_crm
│   │   ├── stg_intercom_messaging
│   │   ├── stg_jira_projects
│   │   ├── stg_mailchimp_email
│   │   ├── stg_mixpanel_events
│   │   ├── stg_segment_events
│   │   ├── stg_stripe_payments
│   │   ├── stg_unknown_values
│   │   └── stg_xero_accounting
│   ├── utils                           <-- "utils" models, for example for row count logging
│   └── warehouse                       <-- "warehouse" models containing fact and dimension tables,
│       ├── w_crm                           grouped by subject area
│       ├── w_finance
│       ├── w_marketing
│       └── w_projects
```

Each data source adapter loads the same columns in the same order for tables that are common to multiple sources, for example:

```
WITH source AS (
  {{ filter_stitch_table(var('users_table'),'gid') }}
  ),

renamed AS (
  SELECT
  concat('{{ var('id-prefix') }}',gid)           as user_id,
  name                   as user_name  ,
  email                  as user_email ,
  cast(null as boolean)         as user_is_contractor,
  case when email like '%@{{ var('staff_email_domain') }}%' then true else false end as user_is_staff,
  cast(null as int64)           as user_weekly_capacity,
  cast(null as string)          as user_phone,
  cast(null as int64)           as user_default_hourly_rate,
  cast(null as int64)           as user_cost_rate,
  true                          as user_is_active,
  cast(null as timestamp)       as user_created_ts,
  cast(null as timestamp)       as user_last_modified_ts
  FROM
    source
  WHERE
    name NOT LIKE 'Private User'
)
SELECT
  *
FROM
  renamed
```

Custom adapters are also provided to provide mappings into these common structures for one-off data sources specific to an implementation, i.e. a custom app database source.

```
WITH source AS (
    select *
    from
    {{ source('custom_source_1','s_transactions' ) }}
),
renamed as (
select
       concat('custom_1-',id)                     as user_id,
       cast(null as string)                     as user_name,
       cast(null as string)                     as user_email,
       cast(null as boolean)                    as user_is_contractor,
       cast(null as boolean)                    as user_is_staff,
       cast(null as numeric)                    as user_weekly_capacity,
       cast(null as string)                     as user_phone,
       cast(null as numeric)                    as user_default_hourly_rate,
       cast(null as numeric)                    as user_cost_rate,
       cast(null as boolean)                    as user_is_active,
       cast(null as timestamp)                  as user_created_ts,
       cast(null as timestamp)                  as user_last_modified_ts
from source)
SELECT
  *
FROM
  renamed
```
