{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','release') }}
),
renamed as (
    select
        cast(id as numeric) as release_id,
        cast(author_id as numeric) as user_id,
        cast(repository_id as numeric) as repository_id,
        cast(body as string) as body,
        cast(created_at as datetime) as created_at,
        cast(draft as boolean) as is_draft,
        cast(name as string) as release_name,
        cast(prerelease as boolean) as is_prerelease,
        cast(published_at as datetime) as published_at,
        cast(tag_name as string) as tag_name,
        cast(target_commitish as string) as target_commitish,
        cast(updated_at as datetime) as updated_at
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
