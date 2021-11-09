{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with commit as (
    select *
    from {{ source('fivetran_github_sources','commit') }}
),
renamed as (
    select
        cast(sha as string) as commit_sha,
        cast(author_date as datetime) as author_date,
        cast(author_email as string) as author_email,
        cast(author_name as string) as author_name,
        cast(committer_date as datetime) as committer_date,
        cast(committer_email as string) as committer_email,
        cast(committer_name as string) as committer_name,
        cast(message as string) as message,
        cast(repository_id as numeric) as repository_id
    from commit c
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
