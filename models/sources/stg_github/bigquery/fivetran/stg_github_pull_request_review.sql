{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','pull_request_review') }}
),
renamed as (
    select
        cast(id as numeric) as review_id,
        cast(body as string) as body,
        cast(commit_sha as string) as commit_sha,
        cast(pull_request_id as numeric) as pull_request_id,
        cast(lower(state) as string) as state,
        cast(submitted_at as datetime) as submitted_at,
        cast(user_id as numeric) as user_id
    from source 
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
