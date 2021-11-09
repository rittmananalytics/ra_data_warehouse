{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','issue_merged') }}
),
renamed as (
    select
        cast(commit_sha as string) as commit_sha,
        cast(issue_id as numeric) as issue_id,
        cast(merged_at as datetime) as merged_at,
        cast(actor_id as numeric) as user_id
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
