{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'asana_projects' in var("crm_warehouse_company_sources") %}


WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_asana_projects_stitch_workspaces_table'),unique_column='gid') }}
),
renamed as (
select concat('{{ var('stg_asana_projects_id-prefix') }}',gid) AS company_id,
    name AS company_name,
    cast (null as string) as company_address,
    cast (null as string) AS company_address2,
    cast (null as string) AS company_city,
    cast (null as string) AS company_state,
    cast (null as string) AS company_country,
    cast (null as string) AS company_zip,
    cast (null as string) AS company_phone,
    cast (null as string) AS company_website,
    cast (null as string) AS company_industry,
    cast (null as string) AS company_linkedin_company_page,
    cast (null as string) AS company_linkedin_bio,
    cast (null as string) AS company_twitterhandle,
    cast (null as string) AS company_description,
    cast (null as string) as company_finance_status,
    cast (null as string) as company_currency_code,
    cast (null as timestamp) as company_created_date,
    cast (null as timestamp) as company_last_modified_date
    from source
where name != 'My Company'
  )
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
