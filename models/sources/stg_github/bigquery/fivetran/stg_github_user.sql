{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with user as (
    select *
    from {{ source('fivetran_github_sources','user') }}
),
renamed as (
    select
        cast(id as numeric) as user_id,
        cast(bio as string) as bio,
        cast(blog as string) as blog,
        cast(company as string) as company,
        cast(created_at as datetime) as created_at,
        cast(login as string) as username,
        cast(name as string) as name,
        cast(type as string) as type,
        cast(location as string) as location,
        cast(updated_at as datetime) as updated_at,
        cast(site_admin as boolean) as is_site_admin,
        cast(hireable as boolean) as is_hireable
    from user u
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
