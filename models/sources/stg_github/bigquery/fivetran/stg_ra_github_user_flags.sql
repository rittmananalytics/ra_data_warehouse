{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with user_flags as (
    select *
    from {{ source('stg_dbt_seed','ra_github_user_flags') }}
),
renamed as (
    select
        cast(github_username as string) as github_username,
        cast(is_rittman as boolean) as is_rittman,
        cast(is_contractor as boolean) as is_contractor
    from user_flags
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
