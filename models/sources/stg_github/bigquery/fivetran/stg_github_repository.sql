{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','repository') }}
),
renamed as (
    select
        cast(id as numeric) as repository_id,
        cast(name as string) as repository_name,
        cast(owner_id as numeric) as owner_user_id,
        cast(full_name as string) as full_name,
        cast(created_at as datetime) as created_at,
        cast(description as string) as description,
        cast(default_branch as string) as default_branch,
        cast(homepage as string) as homepage,
        cast(language as string) as language,
        cast(fork as boolean) as is_fork,
        cast(archived as boolean) as is_archived,
        cast(private as boolean) as is_private
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
